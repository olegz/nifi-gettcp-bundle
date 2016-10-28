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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

@Tags({"get", "fetch", "poll", "tcp", "ingest", "source", "input"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Connects over TCP to the provided server. When receiving data this will writes either the" +
        " full receive buffer or messages based on demarcator to the content of a FlowFile. ")
public final class GetTCP extends AbstractProcessor {

    public static final PropertyDescriptor SERVER_ADDRESS = new PropertyDescriptor
            .Builder().name("Server Address")
            .description("The address of the server to connect to")
            .required(true)
            .addValidator(StandardValidators.URI_VALIDATOR)
            .build();


    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port to connect to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECEIVE_BUFFER_SIZE = new PropertyDescriptor
            .Builder().name("Receive Buffer Size")
            .description("The size of the buffer to receive data in")
            .required(false)
            .defaultValue("2048")
            .addValidator(StandardValidators.createLongValidator(1, 2048, true))
            .build();

    public static final PropertyDescriptor KEEP_ALIVE = new PropertyDescriptor
            .Builder().name("Keep Alive")
            .description("This determines if TCP keep alive is used.")
            .required(false)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The relationship that all sucessful messages from the WebSocket will be sent to")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("The relationship that all failed messages from the WebSocket will be sent to")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(SERVER_ADDRESS);
        _propertyDescriptors.add(PORT);
        _propertyDescriptors.add(RECEIVE_BUFFER_SIZE);
        _propertyDescriptors.add(KEEP_ALIVE);


        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    private SocketChannel client = null;
    private SocketRecveiverThread socketRecveiverThread = null;
    private Future receiverThreadFuture = null;
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    /**
     * Bounded queue of messages events from the socket.
     */
    private final BlockingQueue<String> socketMessagesReceived = new ArrayBlockingQueue<>(256);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {

        //setup the socket
        try {
            client = SocketChannel.open();
            InetSocketAddress inetSocketAddress = new InetSocketAddress(context.getProperty(SERVER_ADDRESS).getValue(),
                    context.getProperty(PORT).asInteger());
            client.setOption(StandardSocketOptions.SO_KEEPALIVE,context.getProperty(KEEP_ALIVE).asBoolean());
            client.setOption(StandardSocketOptions.SO_RCVBUF,context.getProperty(RECEIVE_BUFFER_SIZE).asInteger());
            client.connect(inetSocketAddress);
            client.configureBlocking(false);
            socketRecveiverThread = new SocketRecveiverThread(client,context.getProperty(RECEIVE_BUFFER_SIZE).asInteger(),getLogger());
            if(executorService.isShutdown()){
                executorService = Executors.newFixedThreadPool(1);
            }
            receiverThreadFuture = executorService.submit(socketRecveiverThread);

        } catch (IOException e) {
            throw new ProcessException(e);
        }
    }

    @OnStopped
    public void tearDown() throws ProcessException {
        try {
            socketRecveiverThread.stopProcessing();
            receiverThreadFuture.cancel(true);
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();

                    if (!executorService.awaitTermination(5, TimeUnit.SECONDS))
                        getLogger().error("Executor service for receiver thread did not terminate");
            }
            client.close();
        } catch (final Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        try {

            if(client.isOpen()) {
                final String messages = socketMessagesReceived.poll(100, TimeUnit.MILLISECONDS);

                if (messages == null) {
                    return;
                }

                // final Map<String, String> attributes = new HashMap<>();
                FlowFile flowFile = session.create();

                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(messages.getBytes());
                    }
                });

                session.transfer(flowFile, REL_SUCCESS);

            }
        } catch (InterruptedException exception){
            throw new ProcessException(exception);
        }
    }


    private class SocketRecveiverThread implements Runnable {

        private SocketChannel socketChannel = null;
        private boolean keepProcessing = true;
        private int bufferSize;
        private ComponentLog log;

        SocketRecveiverThread(SocketChannel client, int bufferSize,ComponentLog log) {
            socketChannel = client;
            this.bufferSize = bufferSize;
            this.log= log;
        }

        void stopProcessing(){
            keepProcessing = false;
        }
        public void run() {
            log.debug("Starting to receive messages");

            int nBytes = 0;
            ByteBuffer buf = ByteBuffer.allocate(bufferSize);
            try {
                while (keepProcessing) {
                    if(socketChannel.isOpen() && socketChannel.isConnected()) {
                        while ((nBytes = socketChannel.read(buf)) > 0) {
                            log.debug("Read {} from socket", new Object[]{nBytes});
                            buf.flip();
                            Charset charset = Charset.forName(StandardCharsets.UTF_8.name());
                            CharsetDecoder decoder = charset.newDecoder();
                            CharBuffer charBuffer = decoder.decode(buf);
                            final String message = charBuffer.toString();
                            log.debug("Received Message: {}", new Object[]{message});
                            socketMessagesReceived.offer(message);
                            buf.clear();
                        }
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();

            }


        }
    }
}
