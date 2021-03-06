/**
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.phial.zkclient;


import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoAssertionError;

public class TestUtil {

    /**
     * This waits until the provided {@link Callable} returns an object that is equals to the given expected value or
     * the timeout has been reached. In both cases this method will return the return value of the latest
     * {@link Callable} execution.
     * 
     * @param expectedValue
     *            The expected value of the callable.
     * @param callable
     *            The callable.
     * @param <T>
     *            The return type of the callable.
     * @param timeUnit
     *            The timeout timeunit.
     * @param timeout
     *            The timeout.
     * @return the return value of the latest {@link Callable} execution.
     * @throws Exception
     * @throws InterruptedException
     */
    public static <T> T waitUntil(T expectedValue, Callable<T> callable, TimeUnit timeUnit, long timeout) throws Exception {
        long startTime = System.currentTimeMillis();
        do {
            T actual = callable.call();
            if (expectedValue.equals(actual)) {
                return actual;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                return actual;
            }
            Thread.sleep(50);
        } while (true);
    }

    /**
     * This waits until a mockito verification passed (which is provided in the runnable). This waits until the
     * virification passed or the timeout has been reached. If the timeout has been reached this method will rethrow the
     * {@link MockitoAssertionError} that comes from the mockito verification code.
     * 
     * @param runnable
     *            The runnable containing the mockito verification.
     * @param timeUnit
     *            The timeout timeunit.
     * @param timeout
     *            The timeout.
     * @throws InterruptedException
     */
    public static void waitUntilVerified(Runnable runnable, TimeUnit timeUnit, int timeout) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        do {
            MockitoAssertionError exception = null;
            try {
                runnable.run();
            } catch (MockitoAssertionError e) {
                exception = e;
            }
            if (exception == null) {
                return;
            }
            if (System.currentTimeMillis() > startTime + timeUnit.toMillis(timeout)) {
                throw exception;
            }
            Thread.sleep(50);
        } while (true);
    }

    public static ZkServer startZkServer(Path tempFir, int port) throws IOException {
        Path dataFolder = tempFir.resolveSibling("data");
        Path logFolder = tempFir.resolveSibling("log");
        return startServer(port, dataFolder.toFile().getAbsolutePath(), logFolder.toFile().getAbsolutePath());
    }

    public static ZkServer startZkServer(String testName, int port) throws IOException {
        String dataPath = "./build/test/" + testName + "/data";
        String logPath = "./build/test/" + testName + "/log";
        FileUtils.deleteDirectory(new File(dataPath));
        FileUtils.deleteDirectory(new File(logPath));
        return startServer(port, dataPath, logPath);
    }

    private static ZkServer startServer(int port, String dataPath, String logPath) {
        ZkServer zkServer = new ZkServer(dataPath, logPath, Mockito.mock(IDefaultNameSpace.class), port, ZkServer.DEFAULT_TICK_TIME, 100);
        zkServer.start();
        return zkServer;
    }
}