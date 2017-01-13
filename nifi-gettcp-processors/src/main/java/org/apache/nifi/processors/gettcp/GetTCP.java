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

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@TriggerSerially
@SideEffectFree
@Tags({"get", "fetch", "poll", "tcp", "ingest", "source", "input"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Connects over TCP to the provided endpoint(s). Received data will be written as content to the FlowFile")
@WritesAttribute(attribute = "source.endpoint", description = "The address of the source endpoint the message came from")
public class GetTCP extends AbstractSessionFactoryProcessor {

    private static String SOURCE_ENDPOINT_ATTRIBUTE = "source.endpoint";

    public static final PropertyDescriptor ENDPOINT_LIST = new PropertyDescriptor.Builder()
            .name("endpoint-list")
            .displayName("Endpoint List")
            .description("A comma delimited list of the endpoints to connect to. The format should be " +
                    "<server_address>:<port>. Only one server will be connected to at a time, the others " +
                    "will be used as fail overs.")
            .required(true)
            .addValidator(GetTCPUtils.ENDPOINT_VALIDATOR)
            .build();

    public static final PropertyDescriptor FAILOVER_ENDPOINT = new PropertyDescriptor.Builder()
            .name("failover-endpoint")
            .displayName("Failover Endpoint")
            .description("A failover server to connect to if one of the main ones is unreachable after the connection " +
                    "attempt count. The format should be <server_address>:<port>.")
            .addValidator(GetTCPUtils.ENDPOINT_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECTION_ATTEMPT_COUNT = new PropertyDescriptor.Builder()
            .name("connection-attempt-timeout")
            .displayName("Connection Attempt Count")
            .description("The number of times to try and establish a connection, before using a backup host if available." +
                    " This same attempt count would be used for a backup host as well.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("3")
            .build();

    public static final PropertyDescriptor RECONNECT_INTERVAL = new PropertyDescriptor.Builder()
            .name("reconnect-interval")
            .displayName("Reconnect interval")
            .description("The number of seconds to wait before attempting to reconnect to the endpoint.")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("5 sec")
            .build();

    public static final PropertyDescriptor RECEIVE_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("receive-buffer-size")
            .displayName("Receive Buffer Size")
            .description("The size of the buffer to receive data in")
            .required(false)
            .defaultValue("2048")
            .addValidator(StandardValidators.createLongValidator(1, 2048, true))
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("The relationship that all sucessful messages from the WebSocket will be sent to")
            .build();

    private final static List<PropertyDescriptor> DESCRIPTORS;

    private final static Set<Relationship> RELATIONSHIPS;

    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(ENDPOINT_LIST);
        _propertyDescriptors.add(FAILOVER_ENDPOINT);
        _propertyDescriptors.add(CONNECTION_ATTEMPT_COUNT);
        _propertyDescriptors.add(RECONNECT_INTERVAL);
        _propertyDescriptors.add(RECEIVE_BUFFER_SIZE);

        DESCRIPTORS = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        RELATIONSHIPS = Collections.unmodifiableSet(_relationships);
    }

    private final Map<String, String> dynamicAttributes = new HashMap<>();

    private final Map<String, ReceivingClient> liveTcpClients = new HashMap<>();

    private volatile NiFiDelegatingMessageHandler delegatingMessageHandler;

    private volatile ScheduledThreadPoolExecutor clientScheduler;

    private volatile String originalServerAddressList;

    private volatile String backupServer;

    private volatile int receiveBufferSize;

    private volatile int connectionAttemptCount;

    private volatile long reconnectInterval;

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        this.receiveBufferSize = context.getProperty(RECEIVE_BUFFER_SIZE).asInteger();
        this.originalServerAddressList = context.getProperty(ENDPOINT_LIST).getValue();
        this.backupServer = context.getProperty(FAILOVER_ENDPOINT).getValue();
        this.connectionAttemptCount = context.getProperty(CONNECTION_ATTEMPT_COUNT).asInteger();
        this.reconnectInterval = context.getProperty(RECONNECT_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        this.clientScheduler = new ScheduledThreadPoolExecutor(originalServerAddressList.split(",").length + 1);
        this.clientScheduler.setKeepAliveTime(10, TimeUnit.SECONDS);
        this.clientScheduler.allowCoreThreadTimeOut(true);

        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                this.dynamicAttributes.put(descriptor.getName(), entry.getValue());
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        if (this.delegatingMessageHandler == null) {
            this.delegatingMessageHandler = new NiFiDelegatingMessageHandler(sessionFactory);
        }
        this.run(context);
        context.yield();
    }

    @OnStopped
    public void tearDown() throws ProcessException {
        for (ReceivingClient client : this.liveTcpClients.values()) {
            try {
                client.stop();
            } catch (Exception e) {
                this.getLogger().warn("Failure while stopping client '" + client + "'", e);
            }
        }
        this.liveTcpClients.clear();
        this.clientScheduler.shutdown();
        try {
            if (!this.clientScheduler.awaitTermination(10000, TimeUnit.MILLISECONDS)) {
                this.getLogger().info("Failed to stop client scheduler in 10 sec. Terminating");
                this.clientScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        this.getLogger().info("Processor has successfully shut down");
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder().required(false).name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true).expressionLanguageSupported(true)
                .build();
    }

    private void run(ProcessContext context) {
        String[] serverAddresses = this.originalServerAddressList.split(",");
        for (String hostPortPair : serverAddresses) {
            ReceivingClient client;
            if (!this.liveTcpClients.containsKey(hostPortPair)) {
                String[] hostAndPort = hostPortPair.split(":");
                InetSocketAddress address = new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
                client = new ReceivingClient(address, this.clientScheduler, this.receiveBufferSize);
                client.setReconnectAttempts(this.connectionAttemptCount);
                client.setDelayMillisBeforeReconnect(this.reconnectInterval);
                if (this.backupServer != null) {
                    hostAndPort = this.backupServer.split(":");
                    client.setBackupAddress(new InetSocketAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1])));
                }
                client.setMessageHandler(this.delegatingMessageHandler);
                this.liveTcpClients.put(hostPortPair, client);
                this.startClient(client);
            } else {
                client = this.liveTcpClients.get(hostPortPair);
                if (!client.isRunning()) {
                    client.stop(); // primarily for cleanup in the event of abnormal termination
                    this.startClient(client);
                }
            }
        }
    }

    private void startClient(ReceivingClient client) {
        this.clientScheduler.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    client.start();
                } catch (Exception e) {
                    getLogger().warn("Failed to start listening client. Will attempt to start on another trigger cycle.", e);
                }
            }
        });
    }

    /**
     *
     */
    private class NiFiDelegatingMessageHandler implements MessageHandler {
        private final ProcessSessionFactory sessionFactory;

        NiFiDelegatingMessageHandler(ProcessSessionFactory sessionFactory) {
            this.sessionFactory = sessionFactory;
        }

        @Override
        public void handle(InetSocketAddress sourceAddress, byte[] message) {
            ProcessSession session = this.sessionFactory.createSession();
            FlowFile flowFile = session.create();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(message);
                }
            });
            flowFile = session.putAttribute(flowFile, SOURCE_ENDPOINT_ATTRIBUTE, sourceAddress.toString());
            if (!GetTCP.this.dynamicAttributes.isEmpty()) {
                flowFile = session.putAllAttributes(flowFile, GetTCP.this.dynamicAttributes);
            }
            session.transfer(flowFile, REL_SUCCESS);
            session.commit();
        }
    }
}
