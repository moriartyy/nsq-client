package mtime.mq.nsq;

import java.nio.charset.Charset;
import java.util.List;

public class Commands {


    // ASCII stores a reference to the charset needed for commands
    public static final Charset ASCII = Charset.forName("ascii");

    public static final Command NOP = Commands.instance("NOP");

    // Identify creates a new Command to provide information about the client.  After connecting,
    // it is generally the first message sent.
    //
    // The supplied body should be a map marshaled into JSON to provide some flexibility
    // for this command to evolve over time.
    //
    // See http://nsq.io/clients/tcp_protocol_spec.html#identify for information
    // on the supported options
    public static Command identify(byte[] body) {
        return Commands.instance("IDENTIFY", body);
    }

    public static Command identify(Config config) {
        return Commands.identify(generateIdentificationBody(config).getBytes());
    }

    // Touch creates a new Command to reset the timeout for
    // a given message (by id)
    public static Command touch(byte[] messageID) {
        return Commands.instance("TOUCH " + new String(messageID, ASCII));
    }

    // Finish creates a new Command to indiciate that
    // a given message (by id) has been processed successfully
    public static Command finish(byte[] messageID) {
        return Commands.instance("FIN " + new String(messageID, ASCII));
    }

    // Subscribe creates a new Command to subscribe to the given topic/channel
    public static Command subscribe(String topic, String channel) {
        return Commands.instance("SUB " + topic + " " + channel);
    }

    // StartClose creates a new Command to indicate that the
    // client would like to start a close cycle.  nsqd will no longer
    // send messages to a client in this state and the client is expected
    // finish pending messages and close the connection
    public static Command startClose() {
        return Commands.instance("CLS");
    }

    public static Command requeue(byte[] messageID, long timeoutMillis) {
        return Commands.instance("REQ " + new String(messageID, ASCII) + " " + timeoutMillis);
    }

    // Ready creates a new Command to specify
    // the number of messages a client is willing to receive
    public static Command ready(int rdy) {
        return Commands.instance("RDY " + rdy);
    }

    // Publish creates a new Command to write a message to a given topic
    public static Command publish(String topic, byte[] message) {
        return Commands.instance("PUB " + topic, message);
    }

    // MultiPublish creates a new Command to write more than one message to a given topic
    // (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
    // Note: can only be used with more than 1 bodies!
    public static Command multiPublish(String topic, List<byte[]> bodies) {
        Command cmd = Commands.instance("MPUB " + topic);
        cmd.setData(bodies);
        return cmd;
    }

    public static Command deferredPublish(String topic, byte[] message, int deferMillis) {
        return Commands.instance("DPUB " + topic + " " + deferMillis, message);
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

    static String generateIdentificationBody(Config config) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"client_id\":\"").append(config.getClientId()).append("\", ");
        builder.append("\"hostname\":\"").append(config.getHostname()).append("\", ");
        builder.append("\"feature_negotiation\": true, ");
        if (config.getHeartbeatIntervalInMillis() != null) {
            builder.append("\"heartbeat_interval\":").append(config.getHeartbeatIntervalInMillis()).append(", ");
        }
        if (config.getOutputBufferSize() != null) {
            builder.append("\"output_buffer_size\":").append(config.getOutputBufferSize()).append(", ");
        }
        if (config.getOutputBufferTimeoutMillis() != null) {
            builder.append("\"output_buffer_timeout\":").append(config.getOutputBufferTimeoutMillis()).append(", ");
        }
        if (config.isTlsV1()) {
            builder.append("\"tls_v1\":").append(config.isTlsV1()).append(", ");
        }
        if (config.getCompression() == Config.Compression.SNAPPY) {
            builder.append("\"snappy\": true, ");
        }
        if (config.getCompression() == Config.Compression.DEFLATE) {
            builder.append("\"deflate\": true, ");
        }
        if (config.getDeflateLevel() != null) {
            builder.append("\"deflate_level\":").append(config.getDeflateLevel()).append(", ");
        }
        if (config.getSampleRate() != null) {
            builder.append("\"sample_rate\":").append(config.getSampleRate()).append(", ");
        }
        if (config.getMsgTimeoutMillis() != null) {
            builder.append("\"msg_timeout\":").append(config.getMsgTimeoutMillis()).append(", ");
        }
        builder.append("\"user_agent\": \"").append(config.getUserAgent()).append("\"}");
        return builder.toString();
    }
}
