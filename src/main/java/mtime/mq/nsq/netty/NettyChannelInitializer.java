package mtime.mq.nsq.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public class NettyChannelInitializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel ch) throws Exception {
        NettyHelper.initChannel(ch);
    }
}
