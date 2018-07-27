package mtime.mq.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.ServerAddress;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

@Slf4j
public class HttpLookup implements Lookup {

    private final Set<String> lookupServerAddresses = new ConcurrentSkipListSet<>();
    private final ObjectMapper mapper;

    public HttpLookup() {
        this(Collections.emptySet());
    }

    public HttpLookup(Set<ServerAddress> lookupServerAddresses) {
        this(lookupServerAddresses, new ObjectMapper());
    }

    public HttpLookup(Set<ServerAddress> lookupServerAddresses, ObjectMapper mapper) {
        this.mapper = mapper;
        for (ServerAddress lookupServerAddress : lookupServerAddresses) {
            this.addLookupAddress(lookupServerAddress.getHost(), lookupServerAddress.getPort());
        }
    }

    @Override
    public Set<ServerAddress> lookup(String topic) {
        Set<ServerAddress> addresses = new HashSet<>();
        for (String lookupServerAddress : this.lookupServerAddresses) {
            try {
                String topicEncoded = URLEncoder.encode(topic, StandardCharsets.UTF_8.name());
                JsonNode jsonNode = mapper.readTree(new URL(lookupServerAddress + "/lookup?topic=" + topicEncoded));
                log.debug("Server connection information: {}", jsonNode);
                JsonNode producers = jsonNode.findValue("producers");
                for (JsonNode node : producers) {
                    String host = node.get("broadcast_address").asText();
                    ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
                    addresses.add(address);
                }
            } catch (IOException e) {
                log.warn("Unable to connect to address {} for topic {}, reason: {}", lookupServerAddress, topic, e);
            }
        }

        if (addresses.isEmpty()) {
            log.warn("Unable to connect to any NSQ Lookup servers, servers tried: {} on topic: {}", this.lookupServerAddresses, topic);
        }
        return addresses;
    }

    public void addLookupAddress(String host, int port) {
        this.lookupServerAddresses.add("http://" + host + ":" + port);
    }
}
