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

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.phial.zkclient.util.ZkPathUtil;


public abstract class AbstractConnectionTest {

    private final IZkConnection _connection;

    public AbstractConnectionTest(IZkConnection connection) {
        _connection = connection;
    }

    @Test
    public void testGetChildren_OnEmptyFileSystem() throws KeeperException, InterruptedException {
        InMemoryConnection connection = new InMemoryConnection();
        List<String> children = connection.getChildren("/", false);
        Assertions.assertEquals(0, children.size());
    }

    @Test
    public void testSequential() throws KeeperException, InterruptedException {
        String sequentialPath = _connection.create("/a", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL);
        int firstSequential = Integer.parseInt(sequentialPath.substring(2));
        Assertions.assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), sequentialPath);
        Assertions.assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/a", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL));
        Assertions.assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/a", new byte[0], CreateMode.PERSISTENT_SEQUENTIAL));
        Assertions.assertEquals("/b" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/b", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL));
        Assertions.assertEquals("/b" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/b", new byte[0], CreateMode.PERSISTENT_SEQUENTIAL));
        Assertions.assertEquals("/a" + ZkPathUtil.leadingZeros(firstSequential++, 10), _connection.create("/a", new byte[0], CreateMode.EPHEMERAL_SEQUENTIAL));
    }

}
