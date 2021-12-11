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


import org.apache.log4j.Logger;
import org.junit.jupiter.api.*;

import java.util.concurrent.TimeUnit;

@Nested
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ContentWatcherTest {

    private static final Logger LOG = Logger.getLogger(ContentWatcherTest.class);

    private static final String FILE_NAME = "/ContentWatcherTest";
    private ZkServer _zkServer;
    private ZkClient _zkClient;

    @BeforeAll
    public void setUp() throws Exception {
        LOG.info("------------ BEFORE -------------");
        _zkServer = TestUtil.startZkServer("ContentWatcherTest", 4711);
        _zkClient = _zkServer.getZkClient();
    }

    @AfterAll
    public void tearDown() throws Exception {
        if (_zkServer != null) {
            _zkServer.shutdown();
        }
        LOG.info("------------ AFTER -------------");
    }

    @Test
    public void testGetContent() throws Exception {
        LOG.info("--- testGetContent");
        _zkClient.createPersistent(FILE_NAME, "a");
        final ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
        watcher.start();
        Assertions.assertEquals("a", watcher.getContent());

        // update the content
        _zkClient.writeData(FILE_NAME, "b");

        String contentFromWatcher = TestUtil.waitUntil("b", watcher::getContent, TimeUnit.SECONDS, 5);

        Assertions.assertEquals("b", contentFromWatcher);
        watcher.stop();
    }

    @Test
    public void testGetContentWaitTillCreated() throws InterruptedException {
        LOG.info("--- testGetContentWaitTillCreated");
        final Holder<String> contentHolder = new Holder<String>();

        Thread thread = new Thread(() -> {
            ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
            try {
                watcher.start();
                contentHolder.set(watcher.getContent());
                watcher.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        thread.start();

        // create content after 200ms
        Thread.sleep(200);
        _zkClient.createPersistent(FILE_NAME, "aaa");

        // we give the thread some time to pick up the change
        thread.join(1000);
        Assertions.assertEquals("aaa", contentHolder.get());
    }

    @Test
    public void testHandlingNullContent() throws InterruptedException {
        LOG.info("--- testHandlingNullContent");
        _zkClient.createPersistent(FILE_NAME, null);
        ContentWatcher<String> watcher = new ContentWatcher<String>(_zkClient, FILE_NAME);
        watcher.start();
        Assertions.assertNull(watcher.getContent());
        watcher.stop();
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    public void testHandlingOfConnectionLoss() throws Exception {
        LOG.info("--- testHandlingOfConnectionLoss");
        final Gateway gateway = new Gateway(4712, 4711);
        gateway.start();
        final ZkClient zkClient = new ZkClient("localhost:4712", 30000);

        // disconnect
        gateway.stop();

        // reconnect after 250ms and create file with content
        new Thread(() -> {
            try {
                Thread.sleep(250);
                gateway.start();
                zkClient.createPersistent(FILE_NAME, "aaa");
                zkClient.writeData(FILE_NAME, "b");
            } catch (Exception e) {
                // ignore
            }
        }).start();

        final ContentWatcher<String> watcher = new ContentWatcher<>(zkClient, FILE_NAME);
        watcher.start();
        TestUtil.waitUntil("b", watcher::getContent, TimeUnit.SECONDS, 5);
        Assertions.assertEquals("b", watcher.getContent());

        watcher.stop();
        zkClient.close();
        gateway.stop();
    }
}
