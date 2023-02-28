package com.trendyol.stove.testing.e2e.kafka.proxy

import com.trendyol.stove.functional.Try
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS
import org.apache.kafka.common.requests.AbstractRequest
import org.slf4j.Logger
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.*

class ProxyServer(
    private val onPort: Int = 8080,
    private val targetHost: String,
    private val targetPort: Int,
) {

    private val logger: Logger = org.slf4j.LoggerFactory.getLogger(javaClass)

    fun startProxy1() {
        val upstreamAddress = InetSocketAddress(targetHost, targetPort)
        ServerBootstrap()
            .group(NioEventLoopGroup(), NioEventLoopGroup())
            .channel(NioServerSocketChannel::class.java)
            .childHandler(object : ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                override fun initChannel(ch: io.netty.channel.socket.SocketChannel) {
                    ch.pipeline().addLast(KafkaPacketDecoder(), KafkaPacketForwarder(upstreamAddress))
                }
            })
            .bind(InetSocketAddress("localhost", onPort)).sync()
    }

    fun startProxy2() {
        val selector = Selector.open()
        val serverChannel = ServerSocketChannel.open()
        serverChannel.bind(InetSocketAddress("localhost", onPort))
        serverChannel.configureBlocking(false)
        serverChannel.register(selector, SelectionKey.OP_ACCEPT)

        while (true) {
            selector.select()

            val selectedKeys = selector.selectedKeys()
            val keyIterator = selectedKeys.iterator()

            while (keyIterator.hasNext()) {
                val key = keyIterator.next()

                if (key.isAcceptable) {
                    val serverSocketChannel = key.channel() as ServerSocketChannel
                    val clientChannel = serverSocketChannel.accept()
                    clientChannel.configureBlocking(false)

                    val remoteChannel = SocketChannel.open()
                    remoteChannel.connect(InetSocketAddress(targetHost, targetPort))
                    remoteChannel.configureBlocking(false)

                    clientChannel.register(selector, SelectionKey.OP_READ, remoteChannel)
                    remoteChannel.register(selector, SelectionKey.OP_READ, clientChannel)
                } else if (key.isReadable) {
                    val sourceChannel = key.channel() as SocketChannel
                    val destinationChannel = key.attachment() as SocketChannel

                    val buffer = ByteBuffer.allocate(1024)
                    val bytesRead = sourceChannel.read(buffer)

                    if (bytesRead == -1) {
                        sourceChannel.close()
                        destinationChannel.close()
                    } else {
                        buffer.flip()
                        val bytes = ByteArray(buffer.remaining())
                        buffer.get(bytes)

                        // Forward the data to the other socket
                        val outputBuffer = ByteBuffer.wrap(bytes)
                        ApiKeys.values()
                            .map {
                                Try { AbstractRequest.parseRequest(it, API_VERSIONS.latestVersion(), outputBuffer.asReadOnlyBuffer()) }
                                    .map {
                                        logger.info(it.request.toString(true))
                                    }
                            }
                        destinationChannel.write(outputBuffer)
                    }
                }

                keyIterator.remove()
            }
        }
    }
}
