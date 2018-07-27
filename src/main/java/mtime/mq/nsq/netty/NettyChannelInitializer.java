package mtime.mq.nsq.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import mtime.mq.nsq.Config;

public class NettyChannelInitializer extends ChannelInitializer<Channel> {

    private final Config config;

    public NettyChannelInitializer(Config config) {
        this.config = config;
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        NettyHelper.initChannel(ch, config);
    }
}
