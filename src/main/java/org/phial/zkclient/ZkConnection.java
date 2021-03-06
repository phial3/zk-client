package org.phial.zkclient;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.phial.zkclient.exception.ZkException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkConnection implements IZkConnection {

    private static final Logger LOG = LoggerFactory.getLogger(ZkConnection.class);

    /** It is recommended to use quite large sessions timeouts for ZooKeeper. */
    private static final int DEFAULT_SESSION_TIMEOUT = 30000;

    private ZooKeeper _zk = null;
    private final Lock _zookeeperLock = new ReentrantLock();

    private final String _servers;
    private final int _sessionTimeOut;

    public ZkConnection(String zkServers) {
        this(zkServers, DEFAULT_SESSION_TIMEOUT);
    }

    public ZkConnection(String zkServers, int sessionTimeOut) {
        _servers = zkServers;
        _sessionTimeOut = sessionTimeOut;
    }

    @Override
    public void connect(Watcher watcher) {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                throw new IllegalStateException("zk client has already been started");
            }
            try {
                LOG.debug("Creating new ZookKeeper instance to connect to " + _servers + ".");
                _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
            } catch (IOException e) {
                throw new ZkException("Unable to connect to " + _servers, e);
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    @Override
    public void close() throws InterruptedException {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                LOG.debug("Closing ZooKeeper connected to " + _servers);
                _zk.close();
                _zk = null;
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    @Override
    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException, InterruptedException {
        return _zk.create(path, data, acl, mode);
    }

    @Override
    public void delete(String path) throws InterruptedException, KeeperException {
        _zk.delete(path, -1);
    }

    @Override
    public void delete(String path, int version) throws InterruptedException, KeeperException {
        _zk.delete(path, version);
    }

    @Override
    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return _zk.exists(path, watch) != null;
    }

    @Override
    public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
        return _zk.getChildren(path, watch);
    }

    @Override
    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getData(path, watch, stat);
    }

    public void writeData(String path, byte[] data) throws KeeperException, InterruptedException {
        writeData(path, data, -1);
    }

    @Override
    public void writeData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        _zk.setData(path, data, version);
    }

    @Override
    public Stat writeDataReturnStat(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return _zk.setData(path, data, version);
    }

    @Override
    public States getZookeeperState() {
        return _zk != null ? _zk.getState() : null;
    }

    public ZooKeeper getZookeeper() {
        return _zk;
    }

    @Override
    public long getCreateTime(String path) throws KeeperException, InterruptedException {
        Stat stat = _zk.exists(path, false);
        if (stat != null) {
            return stat.getCtime();
        }
        return -1;
    }

    @Override
    public String getServers() {
        return _servers;
    }

    @Override
    public List<OpResult> multi(Iterable<Op> ops) throws KeeperException, InterruptedException {
        return _zk.multi(ops);
    }

    @Override
    public void addAuthInfo(String scheme, byte[] auth) {
        _zk.addAuthInfo(scheme, auth);
    }

    @Override
    public void setAcl(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
        _zk.setACL(path, acl, version);
    }

    @Override
    public Map.Entry<List<ACL>, Stat> getAcl(String path) throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        List<ACL> acl = _zk.getACL(path, stat);
        return new SimpleEntry(acl, stat);
    }

}
