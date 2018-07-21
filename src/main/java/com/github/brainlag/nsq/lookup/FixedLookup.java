package com.github.brainlag.nsq.lookup;

import com.github.brainlag.nsq.ServerAddress;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * @author walter
 */
public class FixedLookup implements Lookup {

    private final Set<ServerAddress> serverAddresses;

    FixedLookup(ServerAddress... serverAddresses) {
        this(Arrays.asList(serverAddresses));
    }

    FixedLookup(Collection<ServerAddress> serverAddresses) {
        this.serverAddresses = new HashSet<>(serverAddresses);
    }

    @Override
    public Set<ServerAddress> lookup(String topic) {
        return this.serverAddresses;
    }

    @Override
    public void addLookupAddress(String host, int port) {
        throw new UnsupportedOperationException();
    }
}
