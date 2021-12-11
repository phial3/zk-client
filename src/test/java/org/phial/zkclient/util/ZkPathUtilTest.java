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
package org.phial.zkclient.util;

import org.junit.jupiter.api.Assertions;
import org.phial.zkclient.TestUtil;
import org.phial.zkclient.ZkClient;
import org.phial.zkclient.ZkServer;

public class ZkPathUtilTest  {

    protected ZkServer _zkServer;
    protected ZkClient _client;

    public void testToString() throws Exception {
        _zkServer = TestUtil.startZkServer("ZkPathUtilTest", 4711);
        _client = new ZkClient("localhost:4711", 30000);
        final String file1 = "/files/file1";
        final String file2 = "/files/file2";
        final String file3 = "/files/file2/file3";
        _client.createPersistent(file1, true);
        _client.createPersistent(file2, true);
        _client.createPersistent(file3, true);

        String stringRepresentation = ZkPathUtil.toString(_client);
        System.out.println(stringRepresentation);
        System.out.println("-------------------------");
        Assertions.assertTrue(stringRepresentation.contains("file1"));
        Assertions.assertTrue(stringRepresentation.contains("file2"));
        Assertions.assertTrue(stringRepresentation.contains("file3"));

        // path filtering
        stringRepresentation = ZkPathUtil.toString(_client, "/", new ZkPathUtil.PathFilter() {
            @Override
            public boolean showChilds(String path) {
                return !file2.equals(path);
            }
        });
        Assertions.assertTrue(stringRepresentation.contains("file1"));
        Assertions.assertTrue(stringRepresentation.contains("file2"));
        Assertions.assertFalse(stringRepresentation.contains("file3"));

        // start path
        stringRepresentation = ZkPathUtil.toString(_client, file2, ZkPathUtil.PathFilter.ALL);
        Assertions.assertFalse(stringRepresentation.contains("file1"));
        Assertions.assertTrue(stringRepresentation.contains("file2"));
        Assertions.assertTrue(stringRepresentation.contains("file3"));

        _zkServer.shutdown();
    }

    public void testLeadingZeros() throws Exception {
        Assertions.assertEquals("0000000001", ZkPathUtil.leadingZeros(1, 10));
    }
}
