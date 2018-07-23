package com.mtime.mq.nsq.lookup;

import com.mtime.mq.nsq.ServerAddress;

import java.util.Collection;
import java.util.Set;

public interface Lookup {

    Set<ServerAddress> lookup(String topic);

    static Lookup fixed(ServerAddress... serverAddresses) {
        return new FixedLookup(serverAddresses);
    }

    static Lookup fixed(Collection<ServerAddress> serverAddresses) {
        return new FixedLookup(serverAddresses);
    }

}
