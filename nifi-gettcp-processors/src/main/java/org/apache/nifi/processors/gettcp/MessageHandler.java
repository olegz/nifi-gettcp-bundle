package org.apache.nifi.processors.gettcp;

public interface MessageHandler {

    void handle(byte[] message);
}
