package mtime.mq.nsq.netty;

import io.netty.buffer.Unpooled;
import io.netty.util.AttributeKey;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.Command;
import mtime.mq.nsq.Config;
import mtime.mq.nsq.ServerAddress;
import mtime.mq.nsq.channel.AbstractChannel;
import mtime.mq.nsq.channel.Channel;
import mtime.mq.nsq.exceptions.NSQException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hongmiao.yu
 */
@Slf4j
public class NettyChannel extends AbstractChannel implements Channel {
    static final AttributeKey<NettyChannel> CHANNEL_KEY = AttributeKey.newInstance("nettyChannel");
    private static final AtomicLong instanceCount = new AtomicLong();

    private io.netty.channel.Channel channel;

    public static NettyChannel instance(ServerAddress serverAddress, Config config) {
        return instance(NettyHelper.openChannel(serverAddress, config.getSocketThreads()), serverAddress, config);
    }

    public static NettyChannel instance(io.netty.channel.Channel channel, ServerAddress serverAddress, Config config) {
        NettyChannel nettyChannel = new NettyChannel(channel, serverAddress, config);
        nettyChannel.getChannel().write(Unpooled.wrappedBuffer(NettyHelper.MAGIC_PROTOCOL_VERSION));
        nettyChannel.getChannel().flush();
        try {
            nettyChannel.sendAndWait(Command.identify(config));
        } catch (Exception e) {
            throw new NSQException("identify failed", e);
        }
        log.info("NettyChannel created, total: {}", instanceCount.incrementAndGet());
        return nettyChannel;
    }

    private NettyChannel(io.netty.channel.Channel channel, ServerAddress serverAddress, Config config) {
        super(serverAddress, config);
        this.channel = channel;
        channel.attr(CHANNEL_KEY).set(this);
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
        return this.channel.isActive();
    }

    @Override
    protected void doSend(Command command) {
        if (!this.channel.writeAndFlush(command).awaitUninterruptibly().isSuccess()) {
            throw new NSQException("Send command failed");
        }
    }
}
