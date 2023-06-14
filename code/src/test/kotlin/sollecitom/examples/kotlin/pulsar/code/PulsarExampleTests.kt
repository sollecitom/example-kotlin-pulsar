package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin.ensureTopicWorks
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin.newConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin.newProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import sollecitom.examples.kotlin.pulsar.test.utils.*
import strikt.api.expectThat
import strikt.assertions.isEqualTo
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
    fun `using the standard Java Pulsar API`() = runTest(timeout = timeout) {

        val schema = Schema.STRING
        val topic = PulsarTopic.persistent("tenant", "namespace", "some-topic-1")
        pulsarAdmin.ensureTopicWorks(topic = topic, schema = schema)
        val producer = pulsarClient.newProducer(schema).topic(topic.fullName).createAsync().await()
        val consumer = pulsarClient.newConsumer(schema).topic(topic.fullName).subscriptionName("a-subscription-1").subscribeAsync().await()
        val message = "Hello Pulsar!"

        producer.sendAsync(message).await()
        val received = consumer.receiveAsync().await()

        expectThat(received.value).isEqualTo(message)
    }

    @Test
    fun `basic string messages with Pulsar`() = runTest(timeout = timeout) {

        val schema = Schema.STRING
        val topic = PulsarTopic.persistent("tenant", "namespace", "some-topic-2")
        pulsarAdmin.ensureTopicWorks(topic = topic, schema = schema)
        val producer = pulsarClient.newProducer(schema) { topic(topic) }
        val consumer = pulsarClient.newConsumer(schema) { topic(topic).subscriptionName("a-subscription-2") }
        val message = "Hello Pulsar!"

        producer.sendSuspending(message)
        val received = consumer.messages.first()

        expectThat(received.value).isEqualTo(message)
    }
}

suspend fun <T> Producer<T>.sendSuspending(message: T): MessageId = withContext(Dispatchers.VirtualThreads) { send(message) }
suspend fun <T> TypedMessageBuilder<T>.sendSuspending(): MessageId = withContext(Dispatchers.VirtualThreads) { send() }
fun <T> ProducerBuilder<T>.topic(topic: PulsarTopic): ProducerBuilder<T> = topic(topic.fullName)

suspend fun <T> Consumer<T>.receiveSuspending(): Message<T> = withContext(Dispatchers.VirtualThreads) { receive() }
fun <T> ConsumerBuilder<T>.topic(topic: PulsarTopic): ConsumerBuilder<T> = topic(topic.fullName)

val <T> Consumer<T>.messages: Flow<Message<T>>
    get() = flow {
        while (currentCoroutineContext().isActive) {
            val message = receiveSuspending()
            emit(message)
        }
    }