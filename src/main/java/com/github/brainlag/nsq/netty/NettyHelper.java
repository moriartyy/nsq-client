package com.github.brainlag.nsq.netty;

import com.github.brainlag.nsq.ServerAddress;
import com.github.brainlag.nsq.exceptions.NoConnectionsException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * @author hongmiao.yu
 */
public class NettyHelper {

    public static final byte[] MAGIC_PROTOCOL_VERSION = "  V2".getBytes();

    public static Bootstrap createBootstrap(ServerAddress serverAddress) {
        return createBootstrap(serverAddress, 0);
    }

    public static Bootstrap createBootstrap(ServerAddress serverAddress, int nThreads) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup(nThreads));
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.handler(new NettyChannelInitializer());
        bootstrap.remoteAddress(serverAddress.getHost(), serverAddress.getPort());
        return bootstrap;
    }

    public static void initChannel(Channel channel) {
        ChannelPipeline pipeline = channel.pipeline();

        LengthFieldBasedFrameDecoder dec = new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES);
        dec.setSingleDecode(true);

        pipeline.addLast("LengthFieldBasedFrameDecoder", dec);
        pipeline.addLast("NSQDecoder", new NettyDecoder()); // in
        pipeline.addLast("NSQEncoder", new NettyEncoder()); // out
        pipeline.addLast("FeatureDetectionHandler", new NettyFeatureDetectionHandler());
        pipeline.addLast("NSQHandler", new NettyHandler()); // in
    }

    public static Channel openChannel(ServerAddress serverAddress) {
        Bootstrap bootstrap = createBootstrap(serverAddress);
        ChannelFuture future = bootstrap.connect();
        future.awaitUninterruptibly();
        if (!future.isSuccess()) {
            throw new NoConnectionsException("Could not connect to server", future.cause());
        }
        return future.channel();
    }
}
