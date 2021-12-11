/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.phial.zkclient.exception.ZkAuthFailedException;
import org.phial.zkclient.exception.ZkException;
import org.phial.zkclient.exception.ZkTimeoutException;

import javax.security.auth.login.Configuration;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;

public class SaslAuthenticatedTest {
    protected static final Logger LOG = Logger.getLogger(SaslAuthenticatedTest.class);
    static final String ZK_AUTH_PROVIDER = "zookeeper.authProvider.1";
    static final String ZK_ALLOW_FAILED_SASL = "zookeeper.allowSaslFailedClients";

    @TempDir
    public Path _temporaryFolder;

    private int _port = 4700;
    private ZkClient _client;
    private ZkServer _zkServer;
    private String _zkServerContextName = "Server";
    private String _zkClientContextName = "Client";
    private String _userSuperPasswd = "adminpasswd";
    private String _userServerSide = "fpj";
    private String _userClientSide = "fpj";
    private String _userServerSidePasswd = "fpjsecret";
    private String _userClientSidePasswd = "fpjsecret";
    private String _zkModule = "org.apache.zookeeper.server.auth.DigestLoginModule";

    private String createJaasFile() throws IOException {
        Path jaasFile = _temporaryFolder.resolve("jaas.conf");
        FileOutputStream jaasOutputStream = new FileOutputStream(jaasFile.toFile());
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", _zkServerContextName, _zkModule).getBytes());
        jaasOutputStream.write(String.format("\tuser_super=\"%s\"\n", _userSuperPasswd).getBytes());
        jaasOutputStream.write(String.format("\tuser_%s=\"%s\";\n};\n\n", _userServerSide, _userServerSidePasswd).getBytes());
        jaasOutputStream.write(String.format("%s {\n\t%s required\n", _zkClientContextName, _zkModule).getBytes());
        jaasOutputStream.write(String.format("\tusername=\"%s\"\n", _userClientSide).getBytes());
        jaasOutputStream.write(String.format("\tpassword=\"%s\";\n};", _userClientSidePasswd).getBytes());
        jaasOutputStream.close();
        return jaasFile.toFile().getAbsolutePath();
    }

    @BeforeAll
    public void setUp() throws IOException {
        // Reset all variables used for the jaas login file
        _zkServerContextName = "Server";
        _zkClientContextName = "Client";
        _userSuperPasswd = "adminpasswd";
        _userServerSide = "fpj";
        _userClientSide = "fpj";
        _userServerSidePasswd = "fpjsecret";
        _userClientSidePasswd = "fpjsecret";
        _zkModule = "org.apache.zookeeper.server.auth.DigestLoginModule";
    }

    @AfterAll
    public void tearDown() {
        if (_client != null) {
            _client.close();
        }
        if (_zkServer != null) {
            _zkServer.shutdown();
        }
        System.clearProperty(ZK_AUTH_PROVIDER);
        System.clearProperty(ZkClient.JAVA_LOGIN_CONFIG_PARAM);
        Configuration.setConfiguration(null);
    }

    private void bootstrap() throws IOException {
        Configuration.setConfiguration(null);
        String jaasFileName = createJaasFile();
        System.setProperty(ZK_AUTH_PROVIDER, "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");
        System.setProperty(ZkClient.JAVA_LOGIN_CONFIG_PARAM, jaasFileName);
        _zkServer = TestUtil.startZkServer(_temporaryFolder, _port);
        _client = _zkServer.getZkClient();
    }

    private void bootstrapWithAuthFailure() throws IOException {
        _userServerSide = "otheruser";
        bootstrap();
    }

    /**
     * Tests that a connection authenticates successfully.
     *
     * @throws IOException
     */
    @Test
    public void testConnection() throws IOException {
        bootstrap();
        _client.createPersistent("/test", new byte[0], Ids.CREATOR_ALL_ACL);
        Assertions.assertTrue(_client.exists("/test"));
    }

    /**
     * Tests that ZkClient throws an exception in the case ZooKeeper keeps dropping the connection due to authentication
     * failures.
     *
     * @throws IOException
     */
    @Test
    public void testAuthFailure() throws IOException {
        try {
            bootstrapWithAuthFailure();
            Assertions.fail("Expected to fail!");
        } catch (ZkException e) {
            Assertions.assertThrows(ZkTimeoutException.class, () -> System.out.println(""));
        }
    }

    /**
     * Tests that ZkClient spots the AuthFailed event in the case the property to allow failed SASL connections is
     * enabled.
     *
     * @throws IOException
     */
    @Test
    public void testAuthFailure_AllowFailedSasl() throws IOException {
        System.setProperty(ZK_ALLOW_FAILED_SASL, "true");
        try {
            bootstrapWithAuthFailure();
            Assertions.fail("Expected to fail!");
            Assertions.assertThrows(ZkAuthFailedException.class, () -> System.out.println(""));
        } finally {
            System.clearProperty(ZK_ALLOW_FAILED_SASL);
        }
    }

    /**
     * Tests that ZkClient spots the AuthFailed event in the case the property to allow failed SASL connections is
     * enabled.
     *
     * @throws IOException
     */
    @Test
    public void testAuthFailure_DisabledSasl() throws IOException {
        System.setProperty(ZkClient.ZK_SASL_CLIENT, "false");
        try {
            bootstrapWithAuthFailure();
        } finally {
            System.clearProperty(ZkClient.ZK_SASL_CLIENT);
        }
    }

    @Test
    public void testUnauthenticatedClient() throws IOException {
        ZkClient unearthed = null;
        try {
            bootstrap();
            System.clearProperty(ZkClient.JAVA_LOGIN_CONFIG_PARAM);
            System.setProperty("zookeeper.sasl.client", "true");
            unearthed = new ZkClient("localhost:" + _port, 6000);
            unearthed.createPersistent("/test", new byte[0], Ids.OPEN_ACL_UNSAFE);
        } finally {
            if (unearthed != null) {
                unearthed.close();
            }
        }
    }

    @Test
    public void testNoZkJaasFile() throws IOException {
        try {
            _zkClientContextName = "OtherClient";
            _zkServerContextName = "OtherServer";
            bootstrap();
            _client.createPersistent("/test", new byte[0], Ids.OPEN_ACL_UNSAFE);
            Assertions.assertTrue(_client.exists("/test"));
        } catch (ZkAuthFailedException e) {
            Assertions.fail("Caught ZkAuthFailed exception and was not expecting it");
        }
    }
}
