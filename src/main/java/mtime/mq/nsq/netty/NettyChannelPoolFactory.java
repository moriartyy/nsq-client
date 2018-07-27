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
    private final long connectionTimeoutMillis;
    private final int connectionsPerServer;

    public NettyChannelPoolFactory(Config config, long connectionTimeoutMillis, int connectionsPerServer) {
        this.config = config;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
        this.connectionsPerServer = connectionsPerServer;
    }

    @Override
    public ChannelPool create(ServerAddress serverAddress) {
        return new NettyChannelPool(serverAddress, config, connectionTimeoutMillis, connectionsPerServer);
    }
}
