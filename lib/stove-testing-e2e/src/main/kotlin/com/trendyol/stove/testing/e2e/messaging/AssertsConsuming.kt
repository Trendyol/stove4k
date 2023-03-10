package com.trendyol.stove.testing.e2e.messaging

import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

interface AssertsConsuming {
    /**
     * Expects a message to be consumed [atLeastIn] given time
     * Expected message instance should be given with [message]
     *
     *  Kafka Example:
     * ```kotlin
     * TestSystem.instance
     * .kafka()
     * .shouldBeConsumed(message = TestEvent(id= "test-id"))
     * ```
     */
    suspend fun shouldBeConsumed(
        atLeastIn: Duration = 5.seconds,
        message: Any,
    ): MessagingSystem

    /**
     * Expects a predicate over a message type of [T].
     * Use the extension of [Companion.shouldBeConsumedOnCondition] to be able to  pass [T] in a generic way.
     * The method waits until the condition is satisfied, otherwise throws [AssertionError] indicating that `Consuming Failed`
     *
     * Example:
     * ```kotlin
     * TestSystem.instance
     *  .kafka().shouldBeConsumedOnCondition<TestEvent>{ actual ->
     *     actual.id == "id-to-match"
     *  }
     * ```
     */
    suspend fun <T : Any> shouldBeConsumedOnCondition(
        atLeastIn: Duration = 5.seconds,
        condition: (T) -> Boolean,
        clazz: KClass<T>,
    ): MessagingSystem

    companion object {

        /**
         * Extension for [AssertsConsuming.shouldBeConsumedOnCondition] to enable generic invocation as method<[T]>(...)
         */
        suspend inline fun <reified T : Any> AssertsConsuming.shouldBeConsumedOnCondition(
            atLeastIn: Duration = 5.seconds,
            noinline condition: (T) -> Boolean,
        ): MessagingSystem = this.shouldBeConsumedOnCondition(atLeastIn, condition, T::class)
    }
}
