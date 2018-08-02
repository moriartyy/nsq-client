package mtime.mq.nsq;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hongmiao.yu
 */
public class Command {

    private String line;
    private List<byte[]> data = new ArrayList<>();
    private boolean expectedResponse = true;

    Command() { /** no instances */}

    public void addBytes(byte[] bytes) {
        data.add(bytes);
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        this.line = line;
    }

    public List<byte[]> getData() {
        return data;
    }

    public void setData(List<byte[]> data) {
        this.data = data;
    }

    public String toString() {
        return this.getLine();
    }

    public boolean isExpectedResponse() {
        return expectedResponse;
    }

    public Command expectedResponse(boolean expected) {
        this.expectedResponse = expected;
        return this;
    }
}
