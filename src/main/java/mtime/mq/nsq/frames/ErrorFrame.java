package mtime.mq.nsq.frames;

import java.nio.charset.StandardCharsets;

public class ErrorFrame extends Frame {
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
