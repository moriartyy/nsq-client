package mtime.mq.nsq.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.*;
import mtime.mq.nsq.channel.AbstractChannel;
import mtime.mq.nsq.channel.Channel;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class NettyChannel extends AbstractChannel implements Channel {

    static final AttributeKey<NettyChannel> CHANNEL_KEY = AttributeKey.newInstance("nettyChannel");

    private io.netty.channel.Channel channel;

    NettyChannel(io.netty.channel.Channel channel, ServerAddress serverAddress, Config config) {
        super(serverAddress, config);
        this.channel = channel;
        this.channel.write(Unpooled.wrappedBuffer(Constants.MAGIC_IDENTIFIER));
        this.channel.flush();
        this.channel.attr(CHANNEL_KEY).set(this);
        this.identity(config);
    }

    public io.netty.channel.Channel getChannel() {
        return this.channel;
    }

    @Override
    public void close() {
        super.close();
        this.channel.close();
    }

    @Override
    public boolean isConnected() {
        return super.isConnected() && this.channel.isActive();
    }

    @Override
    protected void doSend(Command command, ResponseFuture responseFuture) {
        ChannelFuture channelFuture = this.channel.writeAndFlush(command);
        if (responseFuture != null) {
            channelFuture.addListener(f -> {

                if (!f.isSuccess()) {

                    String errorMessage = getSendFailedMessage(command);

                    if (!responseFuture.isDone()) {
                        responseFuture.set(Response.error(errorMessage));
                    }

                    if (f.cause() != null) {
                        log.error(errorMessage, f.cause());
                    }
                }
            });
        }
    }

    private String getSendFailedMessage(Command command) {
        return "Send command '" + command.getLine() + "' to " + getRemoteAddress() + " failed";
    }
}
