package mtime.mq.nsq.netty;

import mtime.mq.nsq.Config;
import mtime.mq.nsq.ServerAddress;
import mtime.mq.nsq.channel.ChannelPool;
import mtime.mq.nsq.channel.ChannelPoolFactory;

/**
 * @author hongmiao.yu
 */
public class NettyChannelPoolFactory implements ChannelPoolFactory {

    private final Config config;
    private final int connectionsPerServer;

    public NettyChannelPoolFactory(Config config, int connectionsPerServer) {
        this.config = config;
        this.connectionsPerServer = connectionsPerServer;
    }

    @Override
    public ChannelPool create(ServerAddress serverAddress) {
        return new NettyChannelPool(serverAddress, config, connectionsPerServer);
    }
}
