package mtime.mq.nsq.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.ssl.SslHandler;
import lombok.extern.slf4j.Slf4j;
import mtime.mq.nsq.Config;
import mtime.mq.nsq.frames.Frame;
import mtime.mq.nsq.frames.ResponseFrame;

import javax.net.ssl.SSLEngine;

@Slf4j
public class NettyFeatureDetectionHandler extends SimpleChannelInboundHandler<Frame> {

    private final Config config;
    private boolean ssl;
    private boolean compression;
    private boolean snappy;
    private boolean deflate;
    private boolean finished;

    public NettyFeatureDetectionHandler(Config config) {
        this.config = config;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Frame msg) throws Exception {
        log.info("IdentifyResponse: " + new String(msg.getData()));
        boolean reinstallDefaultDecoder = true;
        if (msg instanceof ResponseFrame) {
            ResponseFrame response = (ResponseFrame) msg;
            ChannelPipeline pipeline = ctx.channel().pipeline();
            parseIdentify(response.getMessage());

            if (response.getMessage().equals("OK")) {
                if (finished) {
                    return;
                }
                //round 2
                if (snappy) {
                    reinstallDefaultDecoder = installSnappyDecoder(pipeline);
                }
                if (deflate) {
                    reinstallDefaultDecoder = installDeflateDecoder(pipeline);
                }
                eject(reinstallDefaultDecoder, pipeline);
                if (ssl) {
                    ((SslHandler) pipeline.get("SSLHandler")).setSingleDecode(false);
                }
                return;
            }
            if (ssl) {
                log.info("Adding SSL to pipline");
                SSLEngine sslEngine = this.config.getSslContext().newEngine(ctx.channel().alloc());
                sslEngine.setUseClientMode(true);
                SslHandler sslHandler = new SslHandler(sslEngine, false);
                sslHandler.setSingleDecode(true);
                pipeline.addBefore("LengthFieldBasedFrameDecoder", "SSLHandler", sslHandler);
                if (snappy) {
                    pipeline.addBefore("NSQEncoder", "SnappyEncoder", new SnappyFrameEncoder());
                }
                if (deflate) {
                    pipeline.addBefore("NSQEncoder", "DeflateEncoder", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE,
                            this.config.getDeflateLevel()));
                }
            }
            if (!ssl && snappy) {
                pipeline.addBefore("NSQEncoder", "SnappyEncoder", new SnappyFrameEncoder());
                reinstallDefaultDecoder = installSnappyDecoder(pipeline);
            }
            if (!ssl && deflate) {
                pipeline.addBefore("NSQEncoder", "DeflateEncoder", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.NONE,
                        this.config.getDeflateLevel()));
                reinstallDefaultDecoder = installDeflateDecoder(pipeline);
            }
            if (response.getMessage().contains("version") && finished) {
                eject(reinstallDefaultDecoder, pipeline);
            }
        }
        ctx.fireChannelRead(msg);
    }

    private void eject(final boolean reinstallDefaultDecoder, final ChannelPipeline pipeline) {
        // ok we read only the the first message to set up the pipline, ejecting now!
        pipeline.remove(this);
        if (reinstallDefaultDecoder) {
            log.info("reinstall LengthFieldBasedFrameDecoder");
            pipeline.replace("LengthFieldBasedFrameDecoder", "LengthFieldBasedFrameDecoder",
                    new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, Integer.BYTES));
        }
    }

    private boolean installDeflateDecoder(final ChannelPipeline pipeline) {
        finished = true;
        log.info("Adding deflate to pipline");
        pipeline.replace("LengthFieldBasedFrameDecoder", "DeflateDecoder", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.NONE));
        return false;
    }

    private boolean installSnappyDecoder(final ChannelPipeline pipeline) {
        finished = true;
        log.info("Adding snappy to pipline");
        pipeline.replace("LengthFieldBasedFrameDecoder", "SnappyDecoder", new SnappyFrameDecoder());
        return false;
    }

    private void parseIdentify(final String message) {
        if (message.equals("OK")) {
            return;
        }
        if (message.contains("\"tls_v1\":true")) {
            ssl = true;
        }
        if (message.contains("\"snappy\":true")) {
            snappy = true;
            compression = true;
        }
        if (message.contains("\"deflate\":true")) {
            deflate = true;
            compression = true;
        }
        if (!ssl && !compression) {
            finished = true;
        }
    }
}
