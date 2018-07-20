package com.github.brainlag.nsq.netty;

import com.github.brainlag.nsq.frames.Frame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static com.github.brainlag.nsq.netty.NettyChannel.CHANNEL_KEY;


public class NettyHandler extends SimpleChannelInboundHandler<Frame> {
    protected static final Logger LOGGER = LogManager.getLogger(NettyHandler.class);

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        super.exceptionCaught(ctx, cause);
        LOGGER.error("NSQHandler exception caught", cause);
        ctx.channel().close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws Exception {
        NettyChannel client = ctx.channel().attr(CHANNEL_KEY).get();
        client.receive(frame);
    }
}
