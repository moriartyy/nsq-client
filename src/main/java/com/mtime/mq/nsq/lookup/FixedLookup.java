package com.mtime.mq.nsq.lookup;

import com.mtime.mq.nsq.ServerAddress;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author walter
 */
public class FixedLookup implements Lookup {

    private final Set<ServerAddress> serverAddresses;

    private FixedLookup(Collection<ServerAddress> serverAddresses) {
        this.serverAddresses = new HashSet<>(serverAddresses);
    }

    @Override
    public Set<ServerAddress> lookup(String topic) {
        return this.serverAddresses;
    }

    public static Lookup wrap(ServerAddress... addresses) {
        return new FixedLookup(Arrays.asList(addresses));
    }

    public static Lookup wrap(Collection<ServerAddress> addresses) {
        return new FixedLookup(addresses);
    }
}
