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

import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DistributedQueueTest {

    private ZkServer _zkServer;
    private ZkClient _zkClient;

    @BeforeAll
    public void setUp() throws IOException {
        _zkServer = TestUtil.startZkServer("ZkClientTest-testDistributedQueue", 4711);
        _zkClient = _zkServer.getZkClient();
    }

    @AfterAll
    public void tearDown() {
        if (_zkServer != null) {
            _zkServer.shutdown();
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testDistributedQueue() {
        _zkClient.createPersistent("/queue");

        DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(_zkClient, "/queue");
        distributedQueue.offer(17L);
        distributedQueue.offer(18L);
        distributedQueue.offer(19L);

        Assertions.assertEquals(Long.valueOf(17L), distributedQueue.poll());
        Assertions.assertEquals(Long.valueOf(18L), distributedQueue.poll());
        Assertions.assertEquals(Long.valueOf(19L), distributedQueue.poll());
        Assertions.assertNull(distributedQueue.poll());
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    public void testPeek() {
        _zkClient.createPersistent("/queue");

        DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(_zkClient, "/queue");
        distributedQueue.offer(17L);
        distributedQueue.offer(18L);

        Assertions.assertEquals(Long.valueOf(17L), distributedQueue.peek());
        Assertions.assertEquals(Long.valueOf(17L), distributedQueue.peek());
        Assertions.assertEquals(Long.valueOf(17L), distributedQueue.poll());
        Assertions.assertEquals(Long.valueOf(18L), distributedQueue.peek());
        Assertions.assertEquals(Long.valueOf(18L), distributedQueue.poll());
        Assertions.assertNull(distributedQueue.peek());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMultipleReadingThreads() throws InterruptedException {
        _zkClient.createPersistent("/queue");

        final DistributedQueue<Long> distributedQueue = new DistributedQueue<Long>(_zkClient, "/queue");

        // insert 100 elements
        for (int i = 0; i < 100; i++) {
            distributedQueue.offer((long) i);
        }

        // 3 reading threads
        final Set<Long> readElements = Collections.synchronizedSet(new HashSet<Long>());
        List<Thread> threads = new ArrayList<Thread>();
        final List<Exception> exceptions = new Vector<>();

        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread(() -> {
                try {
                    while (true) {
                        Long value = distributedQueue.poll();
                        if (value == null) {
                            return;
                        }
                        readElements.add(value);
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                    e.printStackTrace();
                }
            });
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        Assertions.assertEquals(0, exceptions.size());
        Assertions.assertEquals(100, readElements.size());
    }
}
