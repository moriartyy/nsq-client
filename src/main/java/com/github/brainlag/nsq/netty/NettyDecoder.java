package com.github.brainlag.nsq.netty;

import com.github.brainlag.nsq.frames.Frame;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class NettyDecoder extends MessageToMessageDecoder<ByteBuf> {

	private int size;
	private Frame frame;

	public NettyDecoder() {
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		size = in.readInt();
		int type = in.readInt();
		frame = Frame.instance(type);
		if (frame == null) {
			//uhh, bad response from server..  what should we do?
			throw new Exception("Bad frame type from server (" + type + ").  disconnect!");
		}
		frame.setSize(size);
		ByteBuf bytes = in.readBytes(frame.getSize() - 4); //subtract 4 because the frame id is included
		if (bytes.hasArray()) {
			frame.setData(bytes.array());
		} else {
			byte[] array = new byte[bytes.readableBytes()];
			bytes.readBytes(array);
			frame.setData(array);
		}
		out.add(frame);
		bytes.release();
	}

}
