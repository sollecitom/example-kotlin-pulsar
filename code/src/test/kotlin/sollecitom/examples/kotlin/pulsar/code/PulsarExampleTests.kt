package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
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
    fun `something with Pulsar`() = runTest(timeout = timeout) {

        val schema = Schema.STRING
        val topic = PulsarTopic.persistent("tenant", "namespace", "some-topic")
        pulsarAdmin.ensureTopicWorks(topic = topic, schema = schema)
        val producer = pulsarClient.newProducer(schema) { topic(topic.fullName) }
        val consumer = pulsarClient.newConsumer(schema) { topic(topic.fullName).subscriptionName("a-subscription") }
        val message = "Hello Pulsar!"

        producer.sendAsync(message).await()
        val received = consumer.messages.first()

        expectThat(received.value).isEqualTo(message)
    }
}

suspend fun <T> Consumer<T>.receiveMessage(): Message<T> = withContext(Dispatchers.VirtualThreads) { receive() }

val <T> Consumer<T>.messages: Flow<Message<T>>
    get() = flow {
        while (currentCoroutineContext().isActive) {
            val message = receiveMessage()
            emit(message)
        }
    }