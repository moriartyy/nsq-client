package mtime.mq.nsq.channel;

import mtime.mq.nsq.ServerAddress;

/**
 * @author hongmiao.yu
 */
@FunctionalInterface
public interface ChannelFactory {

    Channel create(ServerAddress server);

}
