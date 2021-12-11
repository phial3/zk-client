/**
 * Copyright 2010 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.phial.zkclient;

import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.phial.zkclient.exception.ZkBadVersionException;
import org.phial.zkclient.exception.ZkInterruptedException;
import org.phial.zkclient.exception.ZkNoNodeException;
import org.phial.zkclient.exception.ZkTimeoutException;
import org.phial.zkclient.serialize.SerializableSerializer;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ServerZkClientTest extends AbstractBaseZkClientTest {

    private static final int CONNECTION_TIMEOUT = 30000;

    @TempDir
    public Path _temporaryFolder;

    @Override
    @BeforeAll
    public void setUp() throws Exception {
        super.setUp();
        _zkServer = TestUtil.startZkServer(_temporaryFolder, 4711);
        _client = new ZkClient("localhost:4711", CONNECTION_TIMEOUT);
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
        _client.close();
        _zkServer.shutdown();
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testConnectionTimeout() throws Exception {
        LOG.info("--- testConnectionTimeout");
        _zkServer.shutdown();
        new ZkClient("localhost:4711", 500).close();
        Assertions.fail("should throw exception");
        Assertions.assertThrows(ZkTimeoutException.class, () -> System.out.println(""));
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testRetryUntilConnected() throws Exception {
        LOG.info("--- testRetryUntilConnected");
        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();
        final ZkConnection zkConnection = new ZkConnection("localhost:4712");
        final ZkClient zkClient = new ZkClient(zkConnection, CONNECTION_TIMEOUT);

        gateway.stop();

        // start server in 250ms
        new DeferredGatewayStarter(gateway, 250).start();

        // this should work as soon as the connection is reestablished, if it
        // fails it throws a ConnectionLossException
        zkClient.retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                zkConnection.exists("/a", false);
                return null;
            }
        });

        zkClient.close();
        gateway.stop();
    }

    /**
     * Test for reproducing #25 / https://issues.apache.org/jira/browse/KAFKA-824
     *
     * @throws Exception
     */
    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testOperationInRetryLoopWhileClientGetsClosed() throws Exception {
        LOG.info("--- testRetryUntilConnected");
        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();
        final ZkConnection zkConnection = new ZkConnection("localhost:4712");
        final ZkClient zkClient = new ZkClient(zkConnection, CONNECTION_TIMEOUT);

        gateway.stop();
        final Holder<Exception> exceptionHolder = new Holder<Exception>();
        Thread actionThread = new Thread(() -> {
            try {
                zkClient.createPersistent("/root");
            } catch (Exception e) {
                exceptionHolder.set(e);
            }
        });
        actionThread.start();
        zkClient.close();
        actionThread.join();

        Assertions.assertNotNull(exceptionHolder.get());
        Assertions.assertThrows(IllegalStateException.class, () -> System.out.println(""));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    public void testReadWithTimeout() throws Exception {
        final ZkClient zkClient = new ZkClient("localhost:4711", 5000, CONNECTION_TIMEOUT, new SerializableSerializer(), 5000);
        // shutdown the server
        LOG.info("Shutting down zookeeper server " + _zkServer);
        _zkServer.shutdown();
        // now invoke read operation through the client
        try {
            LOG.info("Invoking read on ZkClient when ZK server is down");
            zkClient.readData("/b");
            Assertions.fail("A timeout exception was expected while performing a read through ZkClient, when ZK server is down");
        } catch (ZkTimeoutException zkte) {
            // expected
            LOG.info("Received the *expected* timeout exception while doing an operation through ZkClient", zkte);
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testRetryWithTimeout() throws Exception {
        final ZkClient zkClient = new ZkClient("localhost:4711", CONNECTION_TIMEOUT, CONNECTION_TIMEOUT, new SerializableSerializer(), 5000);
        // shutdown the server
        LOG.info("Shutting down zookeeper server " + _zkServer);
        _zkServer.shutdown();
        // test the retry method directly
        try {
            LOG.info("Invoking retryUntilConnected on ZkClient when ZK server is down");
            zkClient.retryUntilConnected(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return zkClient._connection.exists("/b", true);
                }
            });
            Assertions.fail("A timeout exception was expected while performing an operation through ZkClient with the ZK server down");
        } catch (ZkTimeoutException zkte) {
            // expected
            LOG.info("Received the *expected* timeout exception from ZkClient.retryUntilConnected", zkte);
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testWaitUntilConnected() throws Exception {
        LOG.info("--- testWaitUntilConnected");
        ZkClient _client = new ZkClient("localhost:4711", CONNECTION_TIMEOUT);

        _zkServer.shutdown();

        // the _client state should change to KeeperState.Disconnected
        Assertions.assertTrue(_client.waitForKeeperState(KeeperState.Disconnected, 1, TimeUnit.SECONDS));

        // connection should not be possible and timeout after 100ms
        Assertions.assertFalse(_client.waitUntilConnected(100, TimeUnit.MILLISECONDS));
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testRetryUntilConnected_SessionExpiredException() {
        LOG.info("--- testRetryUntilConnected_SessionExpiredException");

        // Use a tick time of 100ms, because the minimum session timeout is 2 x tick-time.
        // ZkServer zkServer = TestUtil.startZkServer("ZkClientTest-testSessionExpiredException", 4711, 100);
        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();

        // Use a session timeout of 200ms
        final ZkClient zkClient = new ZkClient("localhost:4712", 200, CONNECTION_TIMEOUT);

        gateway.stop();

        // Start server in 600ms, the session should have expired by then
        new DeferredGatewayStarter(gateway, 600).start();

        // This should work as soon as a new session has been created (and the connection is reestablished), if it fails
        // it throws a SessionExpiredException
        zkClient.retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                zkClient.exists("/a");
                return null;
            }
        });

        zkClient.close();
        // zkServer.shutdown();
        gateway.stop();
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testChildListenerAfterSessionExpiredException() throws Exception {
        LOG.info("--- testChildListenerAfterSessionExpiredException");

        int sessionTimeout = 200;
        ZkClient connectedClient = _zkServer.getZkClient();
        connectedClient.createPersistent("/root");

        Gateway gateway = new Gateway(4712, 4711);
        gateway.start();

        final ZkClient disconnectedZkClient = new ZkClient("localhost:4712", sessionTimeout, CONNECTION_TIMEOUT);
        final Holder<List<String>> children = new Holder<List<String>>();
        disconnectedZkClient.subscribeChildChanges("/root", new IZkChildListener() {

            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                children.set(currentChilds);
            }
        });

        gateway.stop();

        // The connected client now created a new child node
        connectedClient.createPersistent("/root/node");

        // Wait for 3 x sessionTimeout, the session should have expired by then and start the gateway again
        Thread.sleep(sessionTimeout * 3);
        gateway.start();

        Boolean hasOneChild = TestUtil.waitUntil(true, new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return children.get() != null && children.get().size() == 1;
            }
        }, TimeUnit.SECONDS, 30);

        Assertions.assertTrue(hasOneChild);

        disconnectedZkClient.close();
        gateway.stop();
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    public void testZkClientConnectedToGatewayClosesQuickly() throws Exception {
        LOG.info("--- testZkClientConnectedToGatewayClosesQuickly");
        final Gateway gateway = new Gateway(4712, 4711);
        gateway.start();

        ZkClient zkClient = new ZkClient("localhost:4712", CONNECTION_TIMEOUT);
        zkClient.close();

        gateway.stop();
    }

    @Test
    public void testCountChildren() throws InterruptedException {
        Assertions.assertEquals(0, _client.countChildren("/a"));
        _client.createPersistent("/a");
        Assertions.assertEquals(0, _client.countChildren("/a"));
        _client.createPersistent("/a/b");
        Assertions.assertEquals(1, _client.countChildren("/a"));

        // test concurrent access
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    while (!isInterrupted()) {
                        _client.createPersistent("/test");
                        _client.delete("/test");
                    }
                } catch (ZkInterruptedException e) {
                    // ignore and finish
                }
            }
        };

        thread.start();
        for (int i = 0; i < 1000; i++) {
            Assertions.assertEquals(0, _client.countChildren("/test"));
        }
        thread.interrupt();
        thread.join();
    }

    @Test
    public void testReadDataWithStat() {
        _client.createPersistent("/a", "data");
        Stat stat = new Stat();
        _client.readData("/a", stat);
        Assertions.assertEquals(0, stat.getVersion());
        Assertions.assertTrue(stat.getDataLength() > 0);
    }

    @Test
    public void testWriteDataWithExpectedVersion() {
        _client.createPersistent("/a", "data");
        _client.writeData("/a", "data2", 0);

        try {
            _client.writeData("/a", "data3", 0);
            Assertions.fail("expected exception");
        } catch (ZkBadVersionException e) {
            // expected
        }
    }

    @Test
    public void testCreateWithParentDirs() {
        String path = "/a/b";
        try {
            _client.createPersistent(path, false);
            Assertions.fail("should throw exception");
        } catch (ZkNoNodeException e) {
            Assertions.assertFalse(_client.exists(path));
        }

        _client.createPersistent(path, true);
        Assertions.assertTrue(_client.exists(path));
    }

    @Test
    public void testUpdateSerialized() throws InterruptedException {
        _client.createPersistent("/a", 0);

        int numberOfThreads = 2;
        final int numberOfIncrementsPerThread = 100;

        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < numberOfThreads; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    for (int j = 0; j < numberOfIncrementsPerThread; j++) {
                        _client.updateDataSerialized("/a", new DataUpdater<Integer>() {

                            @Override
                            public Integer update(Integer integer) {
                                return integer + 1;
                            }
                        });
                    }
                }
            };
            thread.start();
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Integer finalValue = _client.readData("/a");
        Assertions.assertEquals(numberOfIncrementsPerThread * numberOfThreads, finalValue.intValue());
    }
}
