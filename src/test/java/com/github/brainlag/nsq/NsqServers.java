package com.github.brainlag.nsq;

import com.github.brainlag.nsq.lookup.Lookup;

import java.util.HashSet;
import java.util.Set;

/**
 * @author walter
 */
public class NsqServers {

    public static Set<ServerAddress> SUBSCRIBE_SERVERS = new HashSet<>();

    static {
        SUBSCRIBE_SERVERS.add(new ServerAddress("192.168.55.135", 4161));
        SUBSCRIBE_SERVERS.add(new ServerAddress("192.168.55.142", 4161));
    }

    public static Set<ServerAddress> PRODUCE_SERVERS = new HashSet<>();

    static {
        PRODUCE_SERVERS.add(new ServerAddress("192.168.55.135", 4150));
        PRODUCE_SERVERS.add(new ServerAddress("192.168.55.142", 4150));
    }

    public static Lookup SUBSCRIBE_LOOKUP = Lookup.fixed(PRODUCE_SERVERS);

    public static Lookup PRODUCE_LOOKUP = Lookup.fixed(PRODUCE_SERVERS);

}
