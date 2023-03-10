package com.trendyol.stove.testing.e2e.http

import arrow.core.None
import arrow.core.Option
import com.trendyol.stove.testing.e2e.system.abstractions.PluggedSystem
import kotlin.reflect.KClass

/**
 * Http abstraction for testing that does the real calls against the running application
 * @author Oguzhan Soykan
 */
interface HttpSystem : PluggedSystem {

    /**
     * Logs in to the system
     */
    suspend fun login(): HttpSystem

    /**
     * Sends a POST request to the relative [uri] and expects [StoveHttpResponse] without a body
     * Use this method when only the status is the important for your case
     * Provide a [token] in case of Unauthorized with `token = Some("YOUR_TOKEN")`
     *
     * Also: [Companion.postAndExpectBodilessResponse]
     */
    suspend fun postAndExpectBodilessResponse(
        uri: String,
        token: Option<String> = None,
        body: Option<Any> = None,
        expect: suspend (StoveHttpResponse) -> Unit,
    ): HttpSystem

    /**
     * Sends a POST request with [body] to the relative [uri]
     * To skip the body use [None] as value
     *
     * Also: [Companion.postAndExpectJson]
     * */
    suspend fun <TExpected : Any> postAndExpectJson(
        uri: String,
        body: Option<Any>,
        clazz: KClass<TExpected>,
        token: Option<String> = None,
        expect: suspend (actual: TExpected) -> Unit,
    ): HttpSystem

    /**
     * Sends a GET request to the relative [uri] and expects a type of [TExpected] for validation
     * Provide a [token] in case of Unauthorized with `token = Some("YOUR_TOKEN")`
     *
     * Also: [Companion.get]
     */
    suspend fun <TExpected : Any> get(
        uri: String,
        clazz: KClass<TExpected>,
        token: Option<String> = None,
        expect: suspend (TExpected) -> Unit,
    ): HttpSystem

    /**
     * Sends a GET request to the relative [uri] and expects multiple values with the type of [TExpected] for validation
     * Provide a [token] in case of Unauthorized with `token = Some("YOUR_TOKEN")`
     *
     * Also: [Companion.getMany]
     */
    suspend fun <TExpected : Any> getMany(
        uri: String,
        clazz: KClass<TExpected>,
        token: Option<String> = None,
        expect: suspend (List<TExpected>) -> Unit,
    ): HttpSystem

    /**
     * Sends a GET request to the relative [uri] and expects [StoveHttpResponse] without a body
     * Use this method when only the status is the important for your case
     * Provide a [token] in case of Unauthorized with `token = Some("YOUR_TOKEN")`
     *
     * Also: [Companion.getResponse]
     */
    suspend fun getResponse(
        uri: String,
        token: Option<String> = None,
        expect: suspend (StoveHttpResponse) -> Unit,
    ): HttpSystem

    companion object {

        /**
         * Extension for: [HttpSystem.get]
         * */
        suspend inline fun <reified TExpected : Any> HttpSystem.get(
            uri: String,
            token: Option<String> = None,
            noinline expect: suspend (TExpected) -> Unit,
        ): HttpSystem = this.get(uri, TExpected::class, token, expect)

        /**
         * Extension for: [HttpSystem.get]
         * */
        suspend inline fun <reified TExpected : Any> HttpSystem.get(
            uri: String,
            noinline expect: suspend (TExpected) -> Unit,
        ): HttpSystem = this.get(uri, TExpected::class, None, expect)

        /**
         * Extension for: [HttpSystem.getResponse]
         * */
        suspend fun HttpSystem.getResponse(
            uri: String,
            expect: suspend (StoveHttpResponse) -> Unit,
        ): HttpSystem = this.getResponse(uri, None, expect)

        /**
         * Extension for: [HttpSystem.getMany]
         * */
        suspend inline fun <reified TExpected : Any> HttpSystem.getMany(
            uri: String,
            token: Option<String> = None,
            noinline expect: suspend (List<TExpected>) -> Unit,
        ): HttpSystem = this.getMany(uri, TExpected::class, token, expect)

        /**
         * Extension for: [HttpSystem.getMany]
         * */
        suspend inline fun <reified TExpected : Any> HttpSystem.getMany(
            uri: String,
            noinline expect: suspend (List<TExpected>) -> Unit,
        ): HttpSystem = this.getMany(uri, TExpected::class, None, expect)

        /**
         * Extension for: [HttpSystem.postAndExpectJson]
         * */
        suspend inline fun <reified TExpected : Any> HttpSystem.postAndExpectJson(
            uri: String,
            body: Option<Any> = None,
            token: Option<String> = None,
            noinline expect: suspend (actual: TExpected) -> Unit,
        ): HttpSystem = this.postAndExpectJson(uri, body, TExpected::class, token, expect)

        /**
         * Extension for: [HttpSystem.postAndExpectJson]
         * */
        suspend inline fun <reified TExpected : Any> HttpSystem.postAndExpectJson(
            uri: String,
            body: Option<Any> = None,
            noinline expect: suspend (actual: TExpected) -> Unit,
        ): HttpSystem = this.postAndExpectJson(uri, body, TExpected::class, None, expect)

        /**
         * Extension for: [HttpSystem.postAndExpectJson]
         * */
        suspend inline fun <reified TExpected : Any> HttpSystem.postAndExpectJson(
            uri: String,
            noinline expect: suspend (actual: TExpected) -> Unit,
        ): HttpSystem = this.postAndExpectJson(uri, None, TExpected::class, None, expect)

        /**
         * Extension for: [HttpSystem.postAndExpectBodilessResponse]
         * */
        suspend inline fun HttpSystem.postAndExpectBodilessResponse(
            uri: String,
            body: Option<Any> = None,
            noinline expect: suspend (actual: StoveHttpResponse) -> Unit,
        ): HttpSystem = this.postAndExpectBodilessResponse(uri, None, body, expect)

        /**
         * Extension for: [HttpSystem.postAndExpectBodilessResponse]
         * */
        suspend inline fun HttpSystem.postAndExpectBodilessResponse(
            uri: String,
            noinline expect: suspend (actual: StoveHttpResponse) -> Unit,
        ): HttpSystem = this.postAndExpectBodilessResponse(uri, None, None, expect)
    }
}
