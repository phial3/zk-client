package org.phial.zkclient;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.phial.zkclient.testutil.ZkTestSystem;

public class ZkConnectionResolveHostsTest {
    public ZkTestSystem _zk = ZkTestSystem.getInstance();

    @Test
    public void ZkConnectionResolveHosts() {
        String connectionString = "host-unknown,localhost:" + ZkTestSystem.getInstance().getZkServer().getPort();
        IZkConnection connection = new ZkConnection(connectionString);
        new ZkClient(connection);

        String connectionStringWithZkRoot = "host-unknown:5070,localhost:" + ZkTestSystem.getInstance().getZkServer().getPort() + "/zkroot";
        IZkConnection connectionWithZkRoot = new ZkConnection(connectionStringWithZkRoot);
        new ZkClient(connectionWithZkRoot);

        String connectionStringWithSingleHost = "localhost:" + ZkTestSystem.getInstance().getZkServer().getPort();
        IZkConnection connectionWithSingleHost = new ZkConnection(connectionStringWithSingleHost);
        new ZkClient(connectionWithSingleHost);
        Assertions.assertEquals("localhost:" + ZkTestSystem.getInstance().getZkServer().getPort(), connectionWithSingleHost.getServers());
    }

}
