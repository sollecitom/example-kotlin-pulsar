package sollecitom.examples.kotlin.pulsar.test.utils

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.ConsumerBuilder
import org.apache.pulsar.client.api.ProducerBuilder
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy
import org.testcontainers.containers.PulsarContainer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin.ensureTopicWorks
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.KotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.producer.KotlinProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import java.util.*

interface PulsarTestSupport {

    val pulsar: PulsarContainer
    val pulsarClient: PulsarClient
    val pulsarAdmin: PulsarAdmin

    fun startPulsar() {

        pulsar.start()
    }

    fun stopPulsar() {

        pulsarAdmin.close()
        pulsarClient.close()
        pulsar.close()
    }

    fun newPersistentTopic(tenant: String = randomString(), namespace: String = randomString(), name: String = randomString()): PulsarTopic = PulsarTopic.persistent(tenant, namespace, name)

    suspend fun PulsarTopic.ensureWorks(numberOfPartitions: Int = 1, allowTopicCreation: Boolean = false, isAllowAutoUpdateSchema: Boolean = false, schemaValidationEnforced: Boolean = true, schemaCompatibilityStrategy: SchemaCompatibilityStrategy = SchemaCompatibilityStrategy.FULL_TRANSITIVE, schema: Schema<*>? = null): Boolean = pulsarAdmin.ensureTopicWorks(this, numberOfPartitions, allowTopicCreation, isAllowAutoUpdateSchema, schemaValidationEnforced, schemaCompatibilityStrategy, schema)

    suspend fun <T> newProducer(schema: Schema<T>, name: String = randomString(), customize: ProducerBuilder<T>.() -> Unit = {}): KotlinProducer<T> = pulsarClient.newKotlinProducer(schema) { producerName(name).also(customize) }

    suspend fun <T> newConsumer(schema: Schema<T>, subscriptionName: String = randomString(), customize: ConsumerBuilder<T>.() -> Unit = {}): KotlinConsumer<T> = pulsarClient.newKotlinConsumer(schema) { subscriptionName(subscriptionName).also(customize) }

    private fun randomString(): String = UUID.randomUUID().toString()
}