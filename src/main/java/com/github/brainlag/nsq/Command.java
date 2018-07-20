package com.github.brainlag.nsq;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class Command {
    private String line;
    private List<byte[]> data = new ArrayList<>();

    private Command() { /** no instances */}

    public void addBytes(byte[] bytes) {
        data.add(bytes);
    }

    public String getLine() {
        return line;
    }

    public void setLine(String line) {
        if (!line.endsWith("\n")) {
            line = line + "\n";
        }

        this.line = line;
    }

    public List<byte[]> getData() {
        return data;
    }

    public void setData(List<byte[]> data) {
        this.data = data;
    }

    public String toString() {
        return this.getLine().trim();
    }

    // ASCII stores a reference to the charset needed for commands
    public static final Charset ASCII = Charset.forName("ascii");

    public static final Command NOP = Command.instance("NOP");

    // Identify creates a new Command to provide information about the client.  After connecting,
    // it is generally the first message sent.
    //
    // The supplied body should be a map marshaled into JSON to provide some flexibility
    // for this command to evolve over time.
    //
    // See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
    // on the supported options
    public static Command identify(byte[] body) {
        return Command.instance("IDENTIFY", body);
    }

    // Touch creates a new Command to reset the timeout for
    // a given message (by id)
    public static Command touch(byte[] messageID) {
        return Command.instance("TOUCH " + new String(messageID, ASCII));
    }

    // Finish creates a new Command to indiciate that
    // a given message (by id) has been processed successfully
    public static Command finish(byte[] messageID) {
        return Command.instance("FIN " + new String(messageID, ASCII));
    }

    // Subscribe creates a new Command to subscribe to the given topic/channel
    public static Command subscribe(String topic, String channel) {
        return Command.instance("SUB " + topic + " " + channel);
    }

    // StartClose creates a new Command to indicate that the
    // client would like to start a close cycle.  nsqd will no longer
    // send messages to a client in this state and the client is expected
    // finish pending messages and close the connection
    public static Command startClose() {
        return Command.instance("CLS");
    }

    public static Command requeue(byte[] messageID, int timeoutMillis) {
        return Command.instance("REQ " + new String(messageID, ASCII) + " " + timeoutMillis);
    }

    // Ready creates a new Command to specify
    // the number of messages a client is willing to receive
    public static Command ready(int rdy) {
        return Command.instance("RDY " + rdy);
    }

    // Publish creates a new Command to write a message to a given topic
    public static Command publish(String topic, byte[] message) {
        return Command.instance("PUB " + topic, message);
    }

    // MultiPublish creates a new Command to write more than one message to a given topic
    // (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
    // Note: can only be used with more than 1 bodies!
    public static Command multiPublish(String topic, List<byte[]> bodies) {
        Command cmd = Command.instance("MPUB " + topic);
        cmd.setData(bodies);
        return cmd;
    }

    public static Command deferredPublish(String topic, byte[] message, int deferMillis) {
        return Command.instance("DPUB " + topic + " " + deferMillis, message);
    }

    private static Command instance(String line) {
        Command n = new Command();
        n.setLine(line);
        return n;
    }

    private static Command instance(String line, byte[] bytes) {
        Command n = instance(line);
        n.addBytes(bytes);
        return n;
    }

}
