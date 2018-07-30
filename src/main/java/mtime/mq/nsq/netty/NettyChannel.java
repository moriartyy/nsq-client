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
import mtime.mq.nsq.exceptions.NSQException;
import mtime.mq.nsq.exceptions.NSQExceptions;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class NettyChannel extends AbstractChannel implements Channel {

    static final AttributeKey<NettyChannel> CHANNEL_KEY = AttributeKey.newInstance("nettyChannel");

    private static final AtomicLong instanceCount = new AtomicLong();

    private io.netty.channel.Channel channel;

    NettyChannel(io.netty.channel.Channel channel, ServerAddress serverAddress, Config config) {
        super(serverAddress, config);
        this.channel = channel;
        this.channel.write(Unpooled.wrappedBuffer(Constants.MAGIC_IDENTIFIER));
        this.channel.flush();
        channel.attr(CHANNEL_KEY).set(this);
        this.identity(config);
        log.debug("NettyChannel created, server: {} total: {}", serverAddress, instanceCount.incrementAndGet());
    }

    public io.netty.channel.Channel getChannel() {
        return this.channel;
    }

    @Override
    public void close() {
        this.channel.close();
    }

    @Override
    public boolean isConnected() {
        return super.isConnected() && this.channel.isActive();
    }

    @Override
    protected void doSend(Command command, long sendTimeoutMillis) {
        ChannelFuture future = this.channel.writeAndFlush(command);
        if (future.awaitUninterruptibly(sendTimeoutMillis)) {
            if (!future.isSuccess()) {
                throw NSQException.instance("Send command '" + command.getLine() + "' failed to " + getRemoteAddress(), future.cause());
            }
        } else {
            throw NSQExceptions.timeout("Send command '" + command.getLine() + "' timeout", getRemoteAddress());
        }
    }
}
