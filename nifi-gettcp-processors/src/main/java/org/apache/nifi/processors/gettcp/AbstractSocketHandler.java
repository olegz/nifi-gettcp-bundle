/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.gettcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class to implement network Client/Server components
 *
 */
abstract class AbstractSocketHandler {

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final ByteBuffer readingBuffer;

    private final Runnable listenerTask;

    private volatile ExecutorService listenerTaskExecutor;

    final InetSocketAddress address;

    volatile NetworkChannel rootChannel;

    volatile Selector selector;

    private final AtomicBoolean isRunning;

    /**
     *
     * @param address
     * @param server
     */
    public AbstractSocketHandler(InetSocketAddress address, int readingBufferSize) {
        this.address = address;
        this.listenerTask = new ListenerTask();
        this.readingBuffer = ByteBuffer.allocate(readingBufferSize);
        this.isRunning = new AtomicBoolean();
    }

    /**
     *
     * @return
     */
    public void start() {
        if (this.isRunning.compareAndSet(false, true)) {
            try {
                if (this.selector == null || !this.selector.isOpen()) {
                    this.selector = Selector.open();
                    InetSocketAddress connectedAddress = this.connect();
                    this.listenerTaskExecutor = Executors.newCachedThreadPool();
                    this.listenerTaskExecutor.execute(this.listenerTask);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Started listener for " + AbstractSocketHandler.this.getClass().getSimpleName());
                    }
                    if (logger.isInfoEnabled()) {
                        logger.info("Successfully bound to " + connectedAddress);
                    }
                }
            } catch (Exception e) {
                this.stop();
                throw new IllegalStateException("Failed to start " + this.getClass().getName(), e);
            }
        }
    }

    /**
     *
     * @param force
     */
    public void stop() {
        if (this.isRunning.compareAndSet(true, false)) {
            try {
                if (this.selector.isOpen()) { // since stop must be idempotent, we need to check if selector is open to avoid ClosedSelectorException
                    Set<SelectionKey> selectionKeys = new HashSet<>(this.selector.keys());
                    for (SelectionKey key : selectionKeys) {
                        key.cancel();
                        try {
                            key.channel().close();
                        } catch (IOException e) {
                            logger.warn("Failure while closing channel", e);
                        }
                    }
                    try {
                        this.selector.close();
                    } catch (Exception e) {
                        logger.warn("Failure while closinig selector", e);
                    }
                    logger.info(this.getClass().getSimpleName() + " is stopped listening on " + address);
                }
            } finally {
                if (this.listenerTaskExecutor != null) {
                    this.listenerTaskExecutor.shutdown();
                }
            }
        }
    }

    /**
     * Checks if this component is running.
     */
    public boolean isRunning() {
        return this.isRunning.get();
    }

    /**
     *
     * @throws Exception
     */
    abstract InetSocketAddress connect() throws Exception;

    /**
     *
     * @param selectionKey
     * @param buffer
     * @throws IOException
     */
    abstract void read(SelectionKey selectionKey, ByteBuffer buffer) throws IOException;

    /**
     *
     */
    void onDisconnect(Object object) {
        // noop
    }

    /**
     *
     * @param selectionKey
     * @throws IOException
     */
    void doAccept(SelectionKey selectionKey) throws IOException {
        // noop
    }

    /**
     * Main listener task which will process delegate {@link SelectionKey}
     * selected from the {@link Selector} to the appropriate processing method
     * (e.g., accept, read, write etc.)
     */
    private class ListenerTask implements Runnable {
        @Override
        public void run() {
            try {
                while (AbstractSocketHandler.this.rootChannel != null && AbstractSocketHandler.this.rootChannel.isOpen() && AbstractSocketHandler.this.selector.isOpen()) {
                    this.processSelector(10);
                }
            } catch (Exception e) {
                logger.error("Exception in socket listener loop", e);
            }

            logger.debug("Exited Listener loop.");

            AbstractSocketHandler.this.stop();
        }

        /**
         *
         * @param timeout
         * @throws Exception
         */
        private void processSelector(int timeout) throws Exception {
            if (AbstractSocketHandler.this.selector.isOpen() && AbstractSocketHandler.this.selector.select(timeout) > 0) {
                Iterator<SelectionKey> keys = AbstractSocketHandler.this.selector.selectedKeys().iterator();
                processKeys(keys);
            }
        }

        /**
         *
         * @param keys
         */
        private void processKeys(Iterator<SelectionKey> keys) throws IOException {
            while (keys.hasNext()) {
                SelectionKey selectionKey = keys.next();
                keys.remove();
                if (selectionKey.isValid()) {
                    if (selectionKey.isAcceptable()) {
                        this.accept(selectionKey);
                    } else if (selectionKey.isReadable()) {
                        this.read(selectionKey);
                    } else if (selectionKey.isConnectable()) {
                        this.connect(selectionKey);
                    }
                }
            }
        }

        /**
         *
         */
        private void accept(SelectionKey selectionKey) throws IOException {
            AbstractSocketHandler.this.doAccept(selectionKey);
        }

        /**
         *
         */
        private void connect(SelectionKey selectionKey) throws IOException {
            SocketChannel clientChannel = (SocketChannel) selectionKey.channel();
            if (clientChannel.isConnectionPending()) {
                clientChannel.finishConnect();
            }
            clientChannel.register(AbstractSocketHandler.this.selector, SelectionKey.OP_READ);
        }

        /**
         *
         */
        private void read(SelectionKey selectionKey) throws IOException {
            AbstractSocketHandler.this.readingBuffer.clear();
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
            int count = -1;
            try {
                while ((count = socketChannel.read(AbstractSocketHandler.this.readingBuffer)) > 0) {
                    AbstractSocketHandler.this.readingBuffer.flip();
                    byte[] message = new byte[AbstractSocketHandler.this.readingBuffer.limit()];
                    AbstractSocketHandler.this.readingBuffer.get(message);
                    AbstractSocketHandler.this.read(selectionKey, ByteBuffer.wrap(message));
                    AbstractSocketHandler.this.readingBuffer.rewind();
                }
            } catch (IOException e) {
                e.printStackTrace();
                selectionKey.cancel();
                socketChannel.close();
                if (count == -1 && logger.isInfoEnabled()) {
                    logger.info("Connection closed by: " + socketChannel.socket());
                }
            }
            if (count == -1) {
                selectionKey.cancel();
                socketChannel.close();
                logger.info("Connection closed by: " + socketChannel.socket());
            }
        }
    }
}