package mtime.mq.nsq.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.frames.Frame;

@Slf4j
public class NettyHandler extends SimpleChannelInboundHandler<Frame> {

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//        super.exceptionCaught(ctx, cause);
        log.error("NSQHandler exception caught", cause);
        ctx.channel().close();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Frame frame) throws Exception {
        NettyChannel client = ctx.channel().attr(NettyChannel.CHANNEL_KEY).get();
        client.receive(frame);
    }
}
