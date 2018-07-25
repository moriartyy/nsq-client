package mtime.mq.nsq;

import mtime.mq.nsq.lookup.FixedLookup;
import mtime.mq.nsq.lookup.Lookup;

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

    public static Lookup SUBSCRIBE_LOOKUP = FixedLookup.wrap(PRODUCE_SERVERS);

    public static Lookup PRODUCE_LOOKUP = FixedLookup.wrap(PRODUCE_SERVERS);

}
