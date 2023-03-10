@file:Suppress("UNCHECKED_CAST")

package com.trendyol.stove.testing.e2e.couchbase

import com.couchbase.client.core.msg.kv.DurabilityLevel.PERSIST_TO_MAJORITY
import com.couchbase.client.java.*
import com.couchbase.client.java.codec.JacksonJsonSerializer
import com.couchbase.client.java.env.ClusterEnvironment
import com.couchbase.client.java.json.JsonObject
import com.couchbase.client.java.kv.InsertOptions
import com.couchbase.client.java.query.QueryScanConsistency.REQUEST_PLUS
import com.fasterxml.jackson.databind.ObjectMapper
import com.trendyol.stove.functional.Try
import com.trendyol.stove.functional.recover
import com.trendyol.stove.testing.e2e.couchbase.ClusterExtensions.executeQueryAs
import com.trendyol.stove.testing.e2e.database.DocumentDatabaseSystem
import com.trendyol.stove.testing.e2e.system.TestSystem
import com.trendyol.stove.testing.e2e.system.abstractions.ExposesConfiguration
import com.trendyol.stove.testing.e2e.system.abstractions.RunAware
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass

class CouchbaseSystem internal constructor(
    override val testSystem: TestSystem,
    private val context: CouchbaseContext,
) : DocumentDatabaseSystem, RunAware, ExposesConfiguration {

    private lateinit var cluster: ReactiveCluster
    private lateinit var collection: ReactiveCollection
    private lateinit var exposedConfiguration: CouchbaseExposedConfiguration

    private val objectMapper: ObjectMapper = context.options.objectMapper
    private val logger: Logger = LoggerFactory.getLogger(javaClass)

    override suspend fun run() {
        context.container.start()
        val couchbaseHostWithPort = context.container.connectionString.replace("couchbase://", "")
        exposedConfiguration = CouchbaseExposedConfiguration(
            context.container.connectionString,
            couchbaseHostWithPort,
            context.container.username,
            context.container.password
        )
        cluster = createCluster(exposedConfiguration)
        collection = createDefaultCollection()
    }

    override suspend fun stop(): Unit = context.container.stop()

    override fun configuration(): List<String> {
        return context.options.configureExposedConfiguration(exposedConfiguration) +
            listOf(
                "couchbase.hosts=${exposedConfiguration.hostsWithPort}",
                "couchbase.username=${context.container.username}",
                "couchbase.password=${context.container.password}"
            )
    }

    override suspend fun <T : Any> shouldQuery(
        query: String,
        assertion: (List<T>) -> Unit,
        clazz: KClass<T>,
    ): CouchbaseSystem {
        val result = cluster.executeQueryAs<Any>(query) { queryOptions -> queryOptions.scanConsistency(REQUEST_PLUS) }

        val objects = result
            .map { objectMapper.writeValueAsString(it) }
            .map { objectMapper.readValue(it, clazz.java) }

        assertion(objects)
        return this
    }

    override suspend fun <T : Any> shouldGet(
        key: String,
        assertion: (T) -> Unit,
        clazz: KClass<T>,
    ): CouchbaseSystem {
        val result = collection.get(key)
        val expected = result.awaitSingle().contentAs(clazz.java)
        assertion(expected)

        return this
    }

    override suspend fun shouldDelete(key: String): CouchbaseSystem {
        collection.remove(key).awaitSingle()
        return this
    }

    /**
     * Saves the [instance] with given [id] to the [collection]
     * To save to the default collection use [saveToDefaultCollection]
     */
    override suspend fun <T : Any> save(
        collection: String,
        id: String,
        instance: T,
    ): CouchbaseSystem {
        cluster
            .bucket(context.bucket.name)
            .collection(collection)
            .insert(
                id,
                JsonObject.fromJson(objectMapper.writeValueAsString(instance)),
                InsertOptions.insertOptions().durability(PERSIST_TO_MAJORITY)
            )
            .awaitSingle()

        return this
    }

    /**
     * Saves the [instance] with given [id] to the default collection
     * In couchbase the default collection is `_default`
     */
    suspend inline fun <reified T : Any> saveToDefaultCollection(
        id: String,
        instance: T,
    ): CouchbaseSystem = this.save("_default", id, instance)

    override fun close(): Unit = runBlocking {
        Try {
            stop()
            cluster.disconnect().awaitSingle()
        }.recover {
            logger.warn("Disconnecting the couchbase cluster got an error: $it")
        }
    }

    private fun createDefaultCollection(): ReactiveCollection = cluster.bucket(context.bucket.name).defaultCollection()

    private fun createCluster(exposedConfiguration: CouchbaseExposedConfiguration): ReactiveCluster =
        ClusterEnvironment.builder()
            .jsonSerializer(JacksonJsonSerializer.create(objectMapper))
            .build()
            .let {
                Cluster.connect(
                    exposedConfiguration.hostsWithPort,
                    ClusterOptions
                        .clusterOptions(exposedConfiguration.username, exposedConfiguration.password)
                        .environment(it)
                ).reactive()
            }
}
