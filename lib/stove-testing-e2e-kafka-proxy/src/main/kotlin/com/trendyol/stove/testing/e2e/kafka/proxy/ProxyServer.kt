package com.trendyol.stove.testing.e2e.kafka.proxy

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import org.slf4j.Logger

class ProxyServer(
    val onPort: Int = 8080,
    val targetHost: String,
    val targetPort: Int,
) {

    private val logger: Logger = org.slf4j.LoggerFactory.getLogger(javaClass)
    fun work() {
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
                        logger.info(
                            String(outputBuffer.array(), Charsets.US_ASCII)
                        )
                        destinationChannel.write(outputBuffer)
                    }
                }

                keyIterator.remove()
            }
        }
    }
}
