package mtime.mq.nsq.netty;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.Command;
import mtime.mq.nsq.Config;
import mtime.mq.nsq.Constants;
import mtime.mq.nsq.ServerAddress;
import mtime.mq.nsq.channel.AbstractChannel;
import mtime.mq.nsq.channel.Channel;

import java.util.concurrent.CompletableFuture;

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
    protected CompletableFuture<Boolean> doSend(Command command) {
        CompletableFuture<Boolean> r = new CompletableFuture<>();
        ChannelFuture channelFuture = this.channel.writeAndFlush(command);
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                r.complete(Boolean.TRUE);
            } else {
                r.completeExceptionally(f.cause());
                log.error("Send command '" + command.getLine() + "' to " + getRemoteAddress() + " failed", f.cause());
            }
        });
        return r;
    }
}
