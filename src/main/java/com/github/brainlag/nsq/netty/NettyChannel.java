package com.github.brainlag.nsq.netty;

import com.github.brainlag.nsq.*;
import com.github.brainlag.nsq.channel.AbstractChannel;
import com.github.brainlag.nsq.channel.Channel;
import com.github.brainlag.nsq.exceptions.NSQException;
import com.github.brainlag.nsq.frames.ErrorFrame;
import com.github.brainlag.nsq.frames.Frame;
import com.github.brainlag.nsq.frames.MessageFrame;
import com.github.brainlag.nsq.frames.ResponseFrame;
import io.netty.buffer.Unpooled;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hongmiao.yu
 */
public class NettyChannel extends AbstractChannel implements Channel {
    private static final Logger LOGGER = LogManager.getLogger(NettyChannel.class);
    static final AttributeKey<NettyChannel> CHANNEL_KEY = AttributeKey.newInstance("nettyChannel");
    private static final AtomicLong instanceCount = new AtomicLong();

    private io.netty.channel.Channel channel;

    public static NettyChannel instance(io.netty.channel.Channel channel, ServerAddress serverAddress, Config config) {
        LOGGER.info("NettyChannel created, total: {}", instanceCount.incrementAndGet());
        NettyChannel nettyChannel = new NettyChannel(serverAddress, config, channel);

        nettyChannel.getChannel().write(Unpooled.wrappedBuffer(NettyHelper.MAGIC_PROTOCOL_VERSION));
        nettyChannel.getChannel().flush();
        try {
            nettyChannel.sendAndWait(Command.identify(config.toString().getBytes()));
        } catch (Exception e) {
            throw new NSQException("identify failed", e);
        }
        return nettyChannel;
    }

    private NettyChannel(ServerAddress serverAddress, Config config, io.netty.channel.Channel channel) {
        super(serverAddress, config);
        this.channel = channel;
        channel.attr(CHANNEL_KEY).set(this);
    }

    public io.netty.channel.Channel getChannel() {
        return this.channel;
    }

    @Override
    protected void doClose() {
        this.channel.close();
    }

    @Override
    public boolean isConnected() {
        return this.channel.isActive();
    }

    @Override
    protected void doSend(Command command) {
        if (!this.channel.writeAndFlush(command).awaitUninterruptibly().isSuccess()) {
            throw new NSQException("Send command failed");
        }
    }
}
