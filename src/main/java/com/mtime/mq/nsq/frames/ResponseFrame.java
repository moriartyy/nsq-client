package com.mtime.mq.nsq.frames;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class ResponseFrame extends Frame {
    protected static final Logger LOGGER = LogManager.getLogger(ResponseFrame.class);
    private String message;

    @Override
    public void setData(byte[] bytes) {
        //parse the bytes
        super.setData(bytes);
        this.message = new String(getData(), StandardCharsets.UTF_8);
    }

    public String getMessage() {
        return message;
    }

    public String toString() {
        return "RESPONSE: " + this.message;
    }

    public boolean isHeartbeat() {
        return "_heartbeat_".equals(message);
    }
}
