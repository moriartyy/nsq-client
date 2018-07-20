package com.github.brainlag.nsq.lookup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.brainlag.nsq.ServerAddress;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public class DefaultLookup implements Lookup {
    protected static final Logger LOGGER = LogManager.getLogger(DefaultLookup.class);

    private final Set<String> lookupServerAddresses = new ConcurrentSkipListSet<>();
    private final ObjectMapper mapper;

    public DefaultLookup() {
        this(Collections.emptySet());
    }

    public DefaultLookup(Set<ServerAddress> lookupServerAddresses) {
        this(lookupServerAddresses, new ObjectMapper());
    }

    public DefaultLookup(Set<ServerAddress> lookupServerAddresses, ObjectMapper mapper) {
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
                LOGGER.debug("Server connection information: {}", jsonNode);
                JsonNode producers = jsonNode.findValue("producers");
                for (JsonNode node : producers) {
                    String host = node.get("broadcast_address").asText();
                    ServerAddress address = new ServerAddress(host, node.get("tcp_port").asInt());
                    addresses.add(address);
                }
            } catch (IOException e) {
                LOGGER.warn("Unable to connect to address {} for topic {}", lookupServerAddress, topic, e);
            }
        }

        if (addresses.isEmpty()) {
            LOGGER.warn("Unable to connect to any NSQ Lookup servers, servers tried: {} on topic: {}", this.lookupServerAddresses, topic);
        }
        return addresses;
    }

    @Override
    public void addLookupAddress(String host, int port) {
        this.lookupServerAddresses.add("http://" + host + ":" + port);
    }
}
