package com.mtime.mq.nsq.netty;

import com.mtime.mq.nsq.Command;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;

import java.util.List;

public class NettyEncoder extends MessageToMessageEncoder<Command> {

    private static final byte[] NEW_LINE_BYTES = "\n".getBytes();

	@Override
	protected void encode(ChannelHandlerContext ctx, Command message, List<Object> out) throws Exception {
		ByteBuf buf = Unpooled.buffer();
		buf.writeBytes(message.getLine().getBytes("utf8"));
        buf.writeBytes(NEW_LINE_BYTES);

        //for MPUB messages.
		if (message.getData().size() > 1) {
			//write total bodysize and message size
			int bodySize = 4; //4 for total messages int.
			for (byte[] data : message.getData()) {
				bodySize += 4; //message size
				bodySize += data.length;
			}
			buf.writeInt(bodySize);
			buf.writeInt(message.getData().size());
		}
		
		for (byte[] data : message.getData()) {
			buf.writeInt(data.length);
			buf.writeBytes(data);
		}
		out.add(buf);
	}
}
