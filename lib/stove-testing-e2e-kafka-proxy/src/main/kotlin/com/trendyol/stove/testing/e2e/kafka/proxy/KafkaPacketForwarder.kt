package com.trendyol.stove.testing.e2e.kafka.proxy

import io.netty.bootstrap.Bootstrap
import io.netty.channel.*
import io.netty.channel.socket.nio.NioSocketChannel
import java.net.InetSocketAddress

class KafkaPacketForwarder(private val upstreamAddress: InetSocketAddress) : ChannelDuplexHandler() {
    private var upstreamChannel: Channel? = null

    override fun channelActive(ctx: ChannelHandlerContext) {
        val upstreamServer = Bootstrap()
        upstreamServer.group(ctx.channel().eventLoop())
            .channel(ctx.channel().javaClass)
            .handler(object : ChannelInitializer<NioSocketChannel>() {
                override fun initChannel(ch: NioSocketChannel) {
                    ch.pipeline().addLast(
                        KafkaPacketEncoder(),
                        KafkaPacketResponseDecoder(),
                        channelOutboundHandlerAdapter()
                    )
                }
            })

        val connectFuture: ChannelPromise = upstreamServer.connect(upstreamAddress) as DefaultChannelPromise
        connectFuture.addListener { future ->
            when (future.isSuccess) {
                true -> {
                    upstreamChannel = (future as DefaultChannelPromise).channel()
                    println("upstream channel established: $upstreamChannel")
                    // ctx.channel().read()
                }

                false -> {
                    ctx.channel().close()
                }
            }
        }
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        upstreamChannel?.close()
    }

    override fun channelRead(
        ctx: ChannelHandlerContext,
        msg: Any,
    ) {
        upstreamChannel?.let {
            if (it.isActive) {
                it.writeAndFlush(msg)
            }
        }
    }

    override fun write(
        ctx: ChannelHandlerContext,
        msg: Any,
        promise: ChannelPromise,
    ) {
        upstreamChannel?.let {
            if (it.isActive) {
                it.writeAndFlush(msg, promise)
            }
        }
    }

    private fun channelOutboundHandlerAdapter(): ChannelOutboundHandlerAdapter = object : ChannelOutboundHandlerAdapter() {
        override fun write(
            ctx: ChannelHandlerContext,
            msg: Any,
            promise: ChannelPromise,
        ) {
            upstreamChannel?.let {
                if (it.isActive) {
                    it.writeAndFlush(msg, promise)
                }
            }
        }
    }
}
