package com.github.brainlag.nsq.lookup;

import com.github.brainlag.nsq.ServerAddress;

import java.util.Collection;
import java.util.Set;

public interface Lookup {

    Set<ServerAddress> lookup(String topic);

    void addLookupAddress(String host, int port);

    static Lookup fixed(ServerAddress... serverAddresses) {
        return new FixedLookup(serverAddresses);
    }

    static Lookup fixed(Collection<ServerAddress> serverAddresses) {
        return new FixedLookup(serverAddresses);
    }

}
