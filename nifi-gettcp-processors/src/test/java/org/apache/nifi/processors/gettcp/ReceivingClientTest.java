package org.apache.nifi.processors.gettcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReceivingClientTest {

    private ScheduledExecutorService scheduler;

    @Before
    public void before() {
        this.scheduler = Executors.newScheduledThreadPool(1);
    }

    @After
    public void after() {
        this.scheduler.shutdownNow();
    }

    @Test
    public void validateSuccessfullConnectionAndCommunicationToMain() throws Exception {
        String msgToSend = "Hello from validateSuccessfullConnectionAndCommunicationToMain";
        InetSocketAddress address = new InetSocketAddress(9999);
        Server server = new Server(address, 1024);
        server.start();

        ReceivingClient client = new ReceivingClient(address, this.scheduler, 1024);
        AtomicReference<String> receivedMessage = new AtomicReference<String>();
        client.setMessageHandler(message -> receivedMessage.set(new String(message, StandardCharsets.UTF_8)));
        client.start();
        assertTrue(client.isRunning());

        this.sendToSocket(address, msgToSend);
        Thread.sleep(200);
        assertEquals(msgToSend, receivedMessage.get());

        client.stop();
        server.stop();
        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    @Test
    public void validateMessagesWithSomeBiggerThenBuffer() throws Exception {
        String msgToSend = "Hello from validateMessageBiggerThenBuffer";
        InetSocketAddress address = new InetSocketAddress(9999);
        Server server = new Server(address, 30);
        server.start();

        ReceivingClient client = new ReceivingClient(address, this.scheduler, 30);
        List<String> messages = new ArrayList<>();
        client.setMessageHandler(message -> messages.add(new String(message, StandardCharsets.UTF_8)));
        client.start();
        assertTrue(client.isRunning());

        this.sendToSocket(address, msgToSend);
        Thread.sleep(10);
        this.sendToSocket(address, "Hello blah blah");
        Thread.sleep(10);
        this.sendToSocket(address, "foo bar");

        Thread.sleep(200);
        assertEquals(4, messages.size());
        assertEquals("Hello from validateMessageBigg", messages.get(0));
        assertEquals("erThenBuffer", messages.get(1));

        client.stop();
        server.stop();
        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }


    @Test
    public void validateMessageSendBeforeAfterClientConnectDisconnect() throws Exception {
        String msgToSend = "Hello from validateMessageSendAfterClientDisconnect";
        InetSocketAddress address = new InetSocketAddress(9999);
        Server server = new Server(address, 30);
        server.start();
        this.sendToSocket(address, "foo"); // validates no unexpected errors

        ReceivingClient client = new ReceivingClient(address, this.scheduler, 30);
        List<String> messages = new ArrayList<>();
        client.setMessageHandler(message -> messages.add(new String(message, StandardCharsets.UTF_8)));
        client.start();
        assertTrue(client.isRunning());

        this.sendToSocket(address, msgToSend);
        Thread.sleep(200);
        assertEquals(2, messages.size());
        assertEquals("Hello from validateMessageSend", messages.get(0));
        assertEquals("AfterClientDisconnect", messages.get(1));
        messages.clear();

        client.stop();
        this.sendToSocket(address, msgToSend);
        Thread.sleep(200);
        assertEquals(0, messages.size());

        this.sendToSocket(address, msgToSend);

        server.stop();
        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    @Test
    public void validateSuccessfullConnectionAndCommunicationToSecondary() throws Exception {
        String msgToSend = "Hello from validateSuccessfullConnectionAndCommunicationToSecondary";
        InetSocketAddress addressMain = new InetSocketAddress(9998);
        InetSocketAddress addressSecondary = new InetSocketAddress(9999);
        Server server = new Server(addressSecondary, 1024);
        server.start();

        ReceivingClient client = new ReceivingClient(addressMain, this.scheduler, 1024);
        client.setBackupAddress(addressSecondary);
        client.setReconnectAttempts(5);
        client.setDelayMillisBeforeReconnect(200);
        AtomicReference<String> receivedMessage = new AtomicReference<String>();
        client.setMessageHandler(message -> receivedMessage.set(new String(message, StandardCharsets.UTF_8)));
        client.start();
        assertTrue(client.isRunning());

        this.sendToSocket(addressSecondary, msgToSend);
        Thread.sleep(200);
        assertEquals(msgToSend, receivedMessage.get());

        client.stop();
        server.stop();
        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    @Test
    public void validateReconnectDuringReceive() throws Exception {
        String msgToSend = "Hello from validateReconnectDuringReceive";
        InetSocketAddress addressMain = new InetSocketAddress(9998);
        Server server = new Server(addressMain, 1024);
        server.start();

        ExecutorService sendingExecutor = Executors.newSingleThreadExecutor();

        ReceivingClient client = new ReceivingClient(addressMain, this.scheduler, 1024);
        client.setBackupAddress(addressMain);
        client.setReconnectAttempts(10);
        client.setDelayMillisBeforeReconnect(1000);
        client.setDisconnectListener(new DisconnectListener() {
            @Override
            public void onDisconnect(ReceivingClient client) {
                client.stop();
                client.start();
            }
        });
        client.setMessageHandler(message -> System.out.println(new String(message)));
        client.start();
        assertTrue(client.isRunning());

        sendingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    try {
                        sendToSocket(addressMain, msgToSend);
                        Thread.sleep(100);
                    } catch (Exception e) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception ex) {
                            // ignore
                        }
                    }

                }
            }
        });

        Thread.sleep(500);
        server.stop();

        Thread.sleep(500);

        server.start();
        Thread.sleep(1000);

        client.stop();
        server.stop();

        assertFalse(client.isRunning());
        assertFalse(server.isRunning());
    }

    @Test
    public void validateConnectionFailureAfterRetries() throws Exception {
        ReceivingClient client = null;
        try {
            InetSocketAddress addressMain = new InetSocketAddress(9998);
            InetSocketAddress addressSecondary = new InetSocketAddress(9999);

            client = new ReceivingClient(addressMain, this.scheduler, 1024);
            client.setBackupAddress(addressSecondary);
            client.setReconnectAttempts(5);
            client.setDelayMillisBeforeReconnect(200);
            client.start();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }
        assertFalse(client.isRunning());
    }

    private void sendToSocket(InetSocketAddress address, String message) throws Exception {
        Socket socket = new Socket(address.getAddress(), address.getPort());
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.write(message);
        out.flush();
        socket.close();
    }
}
