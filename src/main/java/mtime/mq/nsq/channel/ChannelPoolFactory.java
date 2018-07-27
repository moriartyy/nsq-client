package mtime.mq.nsq.channel;

import mtime.mq.nsq.ServerAddress;

/**
 * @author hongmiao.yu
 */
public interface ChannelPoolFactory {

    ChannelPool create(ServerAddress serverAddress);
}
