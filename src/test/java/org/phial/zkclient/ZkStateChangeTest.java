
package org.phial.zkclient;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


public class ZkStateChangeTest {

    private StateOnlyConnection zkConn;
    private ZkClient client;
    private TestStateListener listener;

    @BeforeAll
    public void setUp() {
        zkConn = new StateOnlyConnection();
        client = new ZkClient(zkConn);
        listener = new TestStateListener();
        client.subscribeStateChanges(listener);
    }

    @AfterAll
    public void tearDown() {
        client.close();
    }

    @Test
    public void testNewSessionEvent() throws Exception {
        zkConn.expireSession();
        assertTimed(1, () -> listener.expiredEvents);

        assertTimed(0, () -> listener.sessionEstablishErrors);

        assertTimed(1, () -> listener.newSessionEvent);
    }

    @Test
    public void testFailConnectEvent() throws Exception {
        zkConn.setFailOnConnect(true);
        zkConn.expireSession();
        assertTimed(1, () -> listener.expiredEvents);

        assertTimed(1, () -> listener.sessionEstablishErrors);

        assertTimed(0, () -> listener.newSessionEvent);

        client.close();
    }

    private <T> void assertTimed(T expectedVal, Callable<T> condition) throws Exception {
        Assertions.assertEquals(expectedVal, TestUtil.waitUntil(expectedVal, condition, TimeUnit.SECONDS, 5));
    }

    private static class StateOnlyConnection implements IZkConnection {
        private Watcher _watcher;
        private boolean failOnConnect = false;

        @Override
        public void connect(Watcher w) {
            _watcher = w;
            if (failOnConnect) {
                // As as example:
                throw new RuntimeException("Testing connection failure");
            }
            new Thread(() -> _watcher.process(new WatchedEvent(null, KeeperState.SyncConnected, null))).start();
        }

        public void expireSession() {
            _watcher.process(new WatchedEvent(null, KeeperState.Expired, null));
        }

        public void setFailOnConnect(boolean failFlag) {
            this.failOnConnect = failFlag;
        }

        @Override
        public void close() throws InterruptedException {

        }

        @Override
        public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void delete(String path) throws InterruptedException, KeeperException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void delete(String path, int version) throws InterruptedException, KeeperException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public boolean exists(final String path, final boolean watch) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public Stat writeDataReturnStat(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public States getZookeeperState() {
            throw new RuntimeException("not implemented");
        }

        @Override
        public long getCreateTime(String path) throws KeeperException, InterruptedException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public String getServers() {
            return "test";
        }

        @Override
        public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addAuthInfo(String scheme, byte[] auth) {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void setAcl(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map.Entry<List<ACL>, Stat> getAcl(String path) throws KeeperException, InterruptedException {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestStateListener implements IZkStateListener {
        public int expiredEvents = 0;
        public int newSessionEvent = 0;
        public int sessionEstablishErrors = 0;

        @Override
        public void handleStateChanged(KeeperState state) throws Exception {
            if (state == KeeperState.Expired) {
                expiredEvents++;
            }
        }

        @Override
        public void handleNewSession() throws Exception {
            newSessionEvent++;
        }

        @Override
        public void handleSessionEstablishmentError(final Throwable error) throws Exception {
            sessionEstablishErrors++;
        }
    }

}
