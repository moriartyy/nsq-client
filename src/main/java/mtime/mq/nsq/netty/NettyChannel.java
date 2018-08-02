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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class NettyChannel extends AbstractChannel implements Channel {

    static final AttributeKey<NettyChannel> CHANNEL_KEY = AttributeKey.newInstance("nettyChannel");

    private static final ConcurrentMap<ServerAddress, AtomicInteger> INSTANCE_COUNTERS = new ConcurrentHashMap<>();

    private io.netty.channel.Channel channel;

    NettyChannel(io.netty.channel.Channel channel, ServerAddress serverAddress, Config config) {
        super(serverAddress, config);
        this.channel = channel;
        this.channel.write(Unpooled.wrappedBuffer(Constants.MAGIC_IDENTIFIER));
        this.channel.flush();
        channel.attr(CHANNEL_KEY).set(this);
        this.identity(config);
        log.info("NettyChannel created, server: {}, current: {}", serverAddress, getCounter(serverAddress).incrementAndGet());
    }

    private AtomicInteger getCounter(ServerAddress serverAddress) {
        return INSTANCE_COUNTERS.computeIfAbsent(serverAddress, s -> new AtomicInteger());
    }

    public io.netty.channel.Channel getChannel() {
        return this.channel;
    }

    @Override
    public void close() {
        log.info("NettyChannel closed, server: {}, current: {}",
                getRemoteAddress(), getCounter(getRemoteAddress()).decrementAndGet());
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
