/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.gettcp;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public final class TestGetTCP {
    private TestRunner testRunner;
    private GetTCP processor;

    @Before
    public void setup() {
        processor = new GetTCP();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testCustomPropertyValidator() {
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999");
        testRunner.assertValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:-1");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, ",");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, ",localhost:9999");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "999,localhost:123");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:abc_port");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999;localhost:1234");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999,localhost:1234");
        testRunner.assertValid();
        testRunner.setProperty(GetTCP.FAILOVER_ENDPOINT, "127.0.0.1;1234");
        testRunner.assertNotValid();
        testRunner.setProperty(GetTCP.FAILOVER_ENDPOINT, "555.0.0.1:1234");
        testRunner.assertValid();
    }

    @Test
    public void testDynamicProperty() {
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:9999,localhost:1234");
        testRunner.setProperty("MyCustomProperty", "abc");
        testRunner.assertValid();
    }

    @Test
    public void testConnection() throws Exception {
        Server server = setupTCPServer(9999);
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:" + 9999);
        testRunner.run(100, false);
        this.sendToSocket(new InetSocketAddress(9999), "Hello");
        Thread.sleep(100);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 1);
        testRunner.clearTransferState();
        testRunner.shutdown();
        server.stop();
    }

    @Test
    public void testFailOver() throws Exception {
        Server server = setupTCPServer(9999);
        testRunner.setProperty(GetTCP.ENDPOINT_LIST, "localhost:" + 9998);
        testRunner.setProperty(GetTCP.FAILOVER_ENDPOINT, "localhost:" + 9999);
        testRunner.setProperty(GetTCP.CONNECTION_ATTEMPT_COUNT, "1");
        testRunner.setProperty(GetTCP.RECONNECT_INTERVAL, "0 millis");
        testRunner.assertValid();
        testRunner.run(1000, false);
        Thread.sleep(200);
        this.sendToSocket(new InetSocketAddress(9999), "Hello");
        Thread.sleep(100);
        testRunner.assertAllFlowFilesTransferred(GetTCP.REL_SUCCESS, 1);
        List<MockFlowFile> files = testRunner.getFlowFilesForRelationship(GetTCP.REL_SUCCESS);
        assertNotNull(files);
        assertTrue(1 == files.size());
        testRunner.clearTransferState();
        testRunner.shutdown();
        server.stop();
    }

    private Server setupTCPServer(int port) {
        InetSocketAddress address = new InetSocketAddress(port);
        Server server = new Server(address, 1024);
        server.start();
        return server;
    }

    private void sendToSocket(InetSocketAddress address, String message) throws Exception {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.write(message);
        out.flush();
        socket.close();
    }
}
