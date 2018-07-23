package com.mtime.mq.nsq.frames;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

public class ErrorFrame extends Frame {
    protected static final Logger LOGGER = LogManager.getLogger(ErrorFrame.class);
    private String errorMessage;

    @Override
    public void setData(byte[] bytes) {
        super.setData(bytes);
        this.errorMessage = new String(getData(), StandardCharsets.UTF_8);
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public String toString() {
        return errorMessage;
    }
}
