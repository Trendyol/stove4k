package com.trendyol.stove.testing.e2e.kafka.proxy

import io.kotest.core.spec.style.FunSpec

class ProxyServerTest : FunSpec({
    test("should work") {
        ProxyServer(8080, "", "localhost", 9200)
            .work()
    }
})
