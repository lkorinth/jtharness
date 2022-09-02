/*
 * $Id$
 *
 * Copyright (c) 2004, 2009, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */
package com.sun.javatest;

import com.sun.javatest.util.BackupPolicy;
import com.sun.javatest.util.I18NResourceBundle;

import static java.lang.Double.compare;
import static java.lang.System.out;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toConcurrentMap;

import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Traditional implementation of the test execution engine which has been
 * used throughout the JT Harness 2.x harness.  It supplies all the basic
 * for creating threads for each test, running the {@code Script},
 * and handling timeouts.
 */
public class DefaultTestRunner extends TestRunner {
    // constants used by classifyThrowable and i18n key unexpectedThrowable
    private static final Integer EXCEPTION = Integer.valueOf(0);
    private static final Integer ERROR = Integer.valueOf(1);
    private static final Integer THROWABLE = Integer.valueOf(2);
    private static I18NResourceBundle i18n = I18NResourceBundle.getBundleForClass(DefaultTestRunner.class);
    private Iterator<TestDescription> testIter;
    private Set<Thread> activeThreads;
    private boolean allPassed;
    private boolean stopping;
    Set<TestDescription> running = ConcurrentHashMap.newKeySet();

    /**
     * A test case is fit to run if the sum of the resources used by
     * <b>running</b> and <b>test</b> is less than the given target.
     *
     * @return true iff resource target is not broken
     */
    private boolean fit(Set<TestDescription> running, TestDescription test, double cpuLoadTarget, long memoryTarget, long maxHeapSize) {
        out.println("lkorinth:"  + test.getName());
        double cpuLoad = running.stream().map(t -> ProcessData.get(t).cpuUsageSecond / ProcessData.get(t).cpuWallSeconds).reduce(0.0, Double::sum);
        long memUsage = running.stream().map(t -> ProcessData.get(t).memUsageInBytes).reduce(0l, Long::sum);

        return ProcessData.get(test).memUsageInBytes + memUsage + maxHeapSize /*sameVM*/ <= memoryTarget
            && ProcessData.get(test).cpuUsageSecond + cpuLoad <= cpuLoadTarget;
    }

    /**
     * The priority is the product of wall time and memory
     * usage. Tests that uses much memory and wall time are hard to
     * schedule, so schedule them first.
     */
    private static double priority(TestDescription test) {
        return ProcessData.get(test).memUsageInBytes * ProcessData.get(test).cpuWallSeconds;
    }

    /**
     * Schedule returns an iterator that gives the order of test cases
     * to run. If we can not reach resource targets, the iterator will
     * block untill enough resources are available. TODO, <b>next()</b> is
     * bussy waiting at the moment, fix this to consume less cpu.
     */
    private Iterator<TestDescription> schedule(Iterator<TestDescription> testIter) {
        if (System.getenv("SCHEDULE") == null) {
            return testIter;
        }

        final long memoryTarget = ofNullable(System.getenv("MAXMEM")).map(Long::valueOf).orElse(8_000_000_000L);
        final double cpuLoadTarget = ofNullable(System.getenv("MAXLOAD")).map(Double::valueOf).orElse(1.0 * getConcurrency());
        final long maxHeapSize = (long) (Runtime.getRuntime().maxMemory() * 1.4);

        List<TestDescription> leftToRun = new ArrayList<>();
        testIter.forEachRemaining(leftToRun::add);
        leftToRun.sort((a, b) -> compare(priority(a), priority(b)));

        return new Iterator<TestDescription>() {
            @Override
            public boolean hasNext() {
                return !leftToRun.isEmpty();
            }
            @Override
            public TestDescription next() {
                TestDescription selected = Stream.generate(() -> {
                        Set<TestDescription> snapshot = new HashSet<>(running);
                        return leftToRun.stream().filter(test -> fit(snapshot, test, cpuLoadTarget, memoryTarget, maxHeapSize)).findFirst();})
                    .flatMap(Optional::stream)
                    .findFirst()
                    .get();

                running.add(selected);
                return selected;
            }
        };
    }

    @Override
    public synchronized boolean runTests(Iterator<TestDescription> testIter)
            throws InterruptedException {
        this.testIter = schedule(testIter);

        Thread[] threads = new Thread[getConcurrency()];
        activeThreads = new HashSet<>();
        allPassed = true;
        try {
            int n = 0;
            while (!stopping) {
                for (int i = 0; i < threads.length; i++) {
                    Thread t = threads[i];
                    if (t == null || !activeThreads.contains(t)) {
                        int prio = Math.max(Thread.MIN_PRIORITY, Thread.currentThread().getPriority() - 1);
                        t = new Thread() {
                            @Override
                            public void run() {
                                try {
                                    TestDescription td;
                                    while ((td = nextTest()) != null) {
                                        if (!runTest(td)) {
                                            running.remove(td);
                                            allPassed = false;
                                        }
                                    }
                                } finally {
                                    // Inform runner this thread is dying, so it can start another thread
                                    // to replace it, if necessary.
                                    threadExiting(this);
                                }
                            }
                        };
                        t.setName("DefaultTestRunner:Worker-" + i + ":" + n++);
                        t.start();
                        t.setPriority(prio);
                        activeThreads.add(t);
                        threads[i] = t;
                    }
                }
                wait();
            }
            // Wait for all the threads to finish so they don't get nuked by the
            // finally code. Order is not important so just wait for them one at a time.
            // Note we can't simply join with the thread because that gives a deadlock
            // on our lock.
            for (int i = 0; i < threads.length; i++) {
                if (threads[i] != null) {
                    while (activeThreads.contains(threads[i])) {
                        wait();
                    }
                    threads[i] = null;
                }
            }
        } catch (InterruptedException ex) {
            // The thread has been interrupted

            stopping = true;    // stop workers from starting any new tests

            // interrupt the worker threads
            for (Thread t : activeThreads) {
                t.interrupt();
            }

            // while a short while (a couple of seconds) for tests to clean up
            // before we nuke them
            long now = System.currentTimeMillis();
            try {
                while (!activeThreads.isEmpty() && (System.currentTimeMillis() - now < 2000)) {
                    wait(100);
                }
            } catch (InterruptedException e) {
            }

            // rethrow the original exception so the caller knows what's happened
            throw ex;
        } finally {
            // ensure all child threads killed
            for (Thread thread : threads) {
                if (thread != null) {
                    Deprecated.invokeThreadStop(thread);
                }
            }
        }

        return allPassed;
    }

    private synchronized void threadExiting(Thread t) {
        activeThreads.remove(t);
        notifyAll();
    }

    private synchronized TestDescription nextTest() {
        if (stopping) {
            return null;
        }

        if (testIter.hasNext()) {
            return testIter.next();
        } else {
            stopping = true;
            return null;
        }
    }

    protected boolean runTest(TestDescription td) {
        WorkDirectory workDir = getWorkDirectory();
        TestResult result = null;

        boolean scriptUsesNotifier = false;

        try {
            TestSuite testSuite = getTestSuite();
            TestEnvironment env = getEnvironment();
            BackupPolicy backupPolicy = getBackupPolicy();

            String[] exclTestCases = getExcludedTestCases(td);
            Script s = testSuite.createScript(td, exclTestCases, env.copy(), workDir, backupPolicy);

            scriptUsesNotifier = s.useNotifier();
            if (!scriptUsesNotifier) {
                notifyStartingTest(s.getTestResult());
            } else {
                delegateNotifier(s);
            }

            result = s.getTestResult();

            //s.run(); lkorinth
        } catch (ThreadDeath e) {
            String url = td.getRootRelativeURL();
            workDir.log(i18n, "dtr.threadKilled", url);
            result = createErrorResult(td, i18n.getString("dtr.threadKilled", url), e);
            throw e;
        } catch (Throwable e) {
            String url = td.getRootRelativeURL();
            workDir.log(i18n, "dtr.unexpectedThrowable",
                    url, e, classifyThrowable(e));
            result = createErrorResult(td,
                    i18n.getString("dtr.unexpectedThrowable",
                            url, e, classifyThrowable(e)),
                    e);
        } finally {
            if (result == null) {
                String url = td.getRootRelativeURL();
                result = createErrorResult(td, i18n.getString("dtr.noResult", url), null);
            }

            if (!scriptUsesNotifier) {
                try {
                    notifyFinishedTest(result);
                } catch (ThreadDeath e) {
                    String url = td.getRootRelativeURL();
                    workDir.log(i18n, "dtr.threadKilled", url);
                    throw e;
                } catch (Throwable e) {
                    String url = td.getRootRelativeURL();
                    workDir.log(i18n, "dtr.unexpectedThrowable", url, e, classifyThrowable(e));
                }
            }
        }

        return result.getStatus().getType() == Status.PASSED;
    }

    private TestResult createErrorResult(TestDescription td, String reason, Throwable t) { // make more i18n
        Status s = Status.error(reason);
        TestResult tr;
        if (t == null) {
            tr = new TestResult(td, s);
        } else {
            tr = new TestResult(td);
            TestResult.Section trs = tr.createSection(i18n.getString("dtr.details"));
            PrintWriter pw = trs.createOutput(i18n.getString("dtr.stackTrace"));
            t.printStackTrace(pw);
            pw.close();
            tr.setStatus(s);
        }

        WorkDirectory workDir = getWorkDirectory();
        BackupPolicy backupPolicy = getBackupPolicy();
        try {
            tr.writeResults(workDir, backupPolicy);
        } catch (Exception e) {
            workDir.log(i18n, "dtr.unexpectedThrowable",
                    td.getRootRelativeURL(), e, EXCEPTION);
        }
        return tr;
    }

    private Integer classifyThrowable(Throwable t) {
        if (t instanceof Exception) {
            return EXCEPTION;
        } else if (t instanceof Error) {
            return ERROR;
        } else {
            return THROWABLE;
        }
    }

    static class ProcessData {
        public int exitCode;
        public double cpuUsageSecond;
        public double cpuWallSeconds;
        public long memUsageInBytes;

        public ProcessData(int exitCode, double cpuUsageSecond, double cpuWallSeconds, long memUsageInBytes) {
            this.exitCode = exitCode;
            this.cpuUsageSecond = cpuUsageSecond;
            this.cpuWallSeconds = cpuWallSeconds;
            this.memUsageInBytes = memUsageInBytes;
        }

        public String toString() {
            return exitCode
                + " " + cpuUsageSecond
                + " " + cpuWallSeconds
                + " " + memUsageInBytes;
        }

        public static Entry<String, ProcessData> fromString(String str) {
            String[] strs = str.split(" ");
            return Map.entry(strs[0], new ProcessData(Integer.valueOf(strs[1]),
                                                      Double.valueOf(strs[2]),
                                                      Double.valueOf(strs[3]),
                                                      Long.valueOf(strs[4])));
        }

        private static Stream<String> lines(Path path) {
            try {
                return Files.lines(path, Charset.forName("UTF-8"));
            } catch (Exception e) {
                e.printStackTrace();
                throw new Error(e);
            }
        }

        private static Map<String, ProcessData> readProcessData(Path p) {
            return lines(p)
                .map(ProcessData::fromString)
                .collect(toConcurrentMap(Entry::getKey, Entry::getValue,
                                         (a, b) -> a.memUsageInBytes > b.memUsageInBytes ? a : b));
        }

        public static Map<String, ProcessData> testProcessData;

        public static ProcessData get(TestDescription td) {
            return testProcessData.get(td.toString()); //TODO WRONG!!!
        }

        static {
            out.println("lkorinth: begin static");
            try {
                testProcessData = readProcessData(Path.of("/home/lkorinth/bisect"));
            } catch (Exception e) {
                out.println("lkorinth catch:" + e);
            }
            out.println("lkorinth: end static");
        }
    }
}
