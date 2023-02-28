package com.trendyol.stove.testing.e2e.kafka.proxy

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

class KafkaPacketEncoder : MessageToByteEncoder<ByteArray>() {
    override fun encode(
        ctx: ChannelHandlerContext,
        msg: ByteArray,
        out: ByteBuf,
    ) {
        out.writeInt(msg.size + 4)
        out.writeBytes(msg)
    }
}
