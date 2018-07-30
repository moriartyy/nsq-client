package mtime.mq.nsq.netty;

import mtime.mq.nsq.Config;
import mtime.mq.nsq.ServerAddress;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.channel.ChannelFactory;

/**
 * @author hongmiao.yu
 */
public class NettyChannelFactory implements ChannelFactory {

    private Config config;

    public NettyChannelFactory(Config config) {
        this.config = config;
    }

    @Override
    public Channel create(ServerAddress server) {
        return new NettyChannel(NettyHelper.openChannel(server, config), server, config);

    }
}
