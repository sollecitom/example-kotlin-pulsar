package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.apache.pulsar.client.api.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin.*
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.topic
import sollecitom.examples.kotlin.pulsar.pulsar.domain.producer.*
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import sollecitom.examples.kotlin.pulsar.test.utils.*
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import java.util.*
import kotlin.time.Duration.Companion.seconds

@TestInstance(PER_CLASS)
private class PulsarExampleTests {

    private val timeout = 10.seconds
    private val pulsar = newPulsarContainer()
    private val pulsarClient by lazy(pulsar::client)
    private val pulsarAdmin by lazy(pulsar::admin)

    @BeforeAll
    fun beforeAll() {
        DebugProbes.install()
        pulsar.start()
    }

    @AfterAll
    fun afterAll() {
        pulsarAdmin.close()
        pulsarClient.close()
        pulsar.stop()
        DebugProbes.uninstall()
    }

    @Test
    fun `sending strings with the standard Java client`() = runTest(timeout = timeout) {

        val schema = Schema.STRING
        val topic = PulsarTopic.persistent("tenant", "namespace", "some-topic-1")
        pulsarAdmin.ensureTopicWorks(topic = topic, schema = schema)
        val producer = pulsarClient.newProducer(schema).topic(topic.fullName).createAsync().await()
        val consumer = pulsarClient.newConsumer(schema).topic(topic.fullName).subscriptionName("a-subscription-1").subscribeAsync().await()
        val message = "Hello Pulsar!"

        producer.sendAsync(message).await()
        val receivedMessage = consumer.receiveAsync().await()
        consumer.acknowledgeAsync(receivedMessage).await()

        expectThat(receivedMessage.value).isEqualTo(message)
    }

    @Test
    fun `sending strings with the Pulsar types`() = runTest(timeout = timeout) {

        val schema = Schema.STRING
        val topic = PulsarTopic.persistent("tenant", "namespace", "some-topic-2")
        pulsarAdmin.ensureTopicWorks(topic = topic, schema = schema)
        val producer = pulsarClient.newKotlinProducer(schema) { topic(topic) }
        val consumer = pulsarClient.newKotlinConsumer(schema) { topic(topic).subscriptionName("a-subscription-2") }
        val message = "Hello Pulsar!"

        producer.send(message)
        val receivedMessage = consumer.messages.first()
        consumer.acknowledge(receivedMessage)

        expectThat(receivedMessage.value).isEqualTo(message)
    }

    @Nested
    inner class WithTestSupport : PulsarTestSupport {

        @Test
        fun `sending strings with the Pulsar types`() = runTest(timeout = timeout) {

            val schema = Schema.STRING
            val topic = newPersistentTopic().also { it.ensureWorks(schema = schema) }
            val producer = newProducer(schema) { topic(topic) }
            val consumer = newConsumer(schema) { topic(topic) }

            val message = "Hello Pulsar!"

            producer.send(message)
            val receivedMessage = consumer.messages.first()
            consumer.acknowledge(receivedMessage)

            expectThat(receivedMessage.value).isEqualTo(message)
        }

        override val pulsar by this@PulsarExampleTests::pulsar
        override val pulsarClient by this@PulsarExampleTests::pulsarClient
        override val pulsarAdmin by this@PulsarExampleTests::pulsarAdmin
    }
}