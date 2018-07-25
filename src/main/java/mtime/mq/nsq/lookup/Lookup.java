package mtime.mq.nsq.lookup;

import mtime.mq.nsq.ServerAddress;

import java.util.Set;

public interface Lookup {

    Set<ServerAddress> lookup(String topic);

}
