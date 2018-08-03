package mtime.mq.nsq;

import java.util.List;

public class Commands {

    public static final Command NOP = Command.instance("NOP").build();

    public static Command identify(byte[] body) {
        return Command.instance("IDENTIFY").data(body).responsive(true).build();
    }

    public static Command identify(Config config) {
        return identify(generateIdentificationBody(config).getBytes());
    }

    // Touch creates a new Command to reset the timeout for
    // a given message (by id)
    public static Command touch(byte[] messageID) {
        return Command.instance("TOUCH").addArgument(messageID).build();
    }

    // Finish creates a new Command to indiciate that
    // a given message (by id) has been processed successfully
    public static Command finish(byte[] messageID) {
        return Command.instance("FIN").addArgument(messageID).build();
    }

    // Subscribe creates a new Command to subscribe to the given topic/channel
    public static Command subscribe(String topic, String channel) {
        return Command.instance("SUB").addArgument(topic, channel).responsive(true).build();
    }

    // StartClose creates a new Command to indicate that the
    // client would like to start a close cycle.  nsqd will no longer
    // send messages to a client in this state and the client is expected
    // finish pending messages and close the connection
    public static Command startClose() {
        return Command.instance("CLS").build();
    }

    public static Command requeue(byte[] messageID, long timeoutMillis) {
        return Command.instance("REQ").addArgument(messageID).addArgument(timeoutMillis).build();
    }

    // Ready creates a new Command to specify
    // the number of messages a client is willing to receive
    public static Command ready(int rdy) {
        return Command.instance("RDY").addArgument(rdy).build();
    }

    // Publish creates a new Command to write a message to a given topic
    public static Command publish(String topic, byte[] message) {
        return Command.instance("PUB").addArgument(topic).data(message).responsive(true).build();
    }

    // MultiPublish creates a new Command to write more than one message to a given topic
    // (useful for high-throughput situations to avoid roundtrips and saturate the pipe)
    // Note: can only be used with more than 1 bodies!
    public static Command multiPublish(String topic, List<byte[]> messages) {
        return Command.instance("MPUB").addArgument(topic).data(messages).responsive(true).build();
    }

    public static Command deferredPublish(String topic, byte[] message, int deferMillis) {
        return Command.instance("DPUB").addArgument(topic).addArgument(deferMillis).data(message).responsive(true).build();
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
