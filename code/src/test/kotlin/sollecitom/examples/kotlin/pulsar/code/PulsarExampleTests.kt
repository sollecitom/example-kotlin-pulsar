package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.*
import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.api.SubscriptionType.Key_Shared
import org.junit.jupiter.api.*
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin.*
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.KotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.topic
import sollecitom.examples.kotlin.pulsar.pulsar.domain.producer.*
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import sollecitom.examples.kotlin.pulsar.test.utils.*
import strikt.api.expectThat
import strikt.assertions.doesNotContain
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import java.io.Closeable
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
    fun `sending strings using the standard Java client`() = runTest(timeout = timeout) {

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
    fun `sending strings using the Kotlin types`() = runTest(timeout = timeout) {

        val schema = Schema.STRING
        val topic = PulsarTopic.persistent("tenant", "namespace", "some-topic-2")
        pulsarAdmin.ensureTopicWorks(topic = topic, schema = schema)
        val producer = pulsarClient.newKotlinProducer(schema) { topic(topic) }
        val consumer = pulsarClient.newKotlinConsumer(schema) { topic(topic).subscriptionName("a-subscription-2") }
        val message = "Hello Pulsar!"

        producer.send(message)
        val receivedMessage = consumer.messages().first()
        consumer.acknowledge(receivedMessage)

        expectThat(receivedMessage.value).isEqualTo(message)
    }

    @Nested
    inner class WithTestSupport : PulsarTestSupport {

        override val pulsar by this@PulsarExampleTests::pulsar
        override val pulsarClient by this@PulsarExampleTests::pulsarClient
        override val pulsarAdmin by this@PulsarExampleTests::pulsarAdmin

        @Test
        fun `sending strings using the Kotlin types`() = runTest(timeout = timeout) {

            val schema = Schema.STRING
            val topic = newPersistentTopic().also { it.ensureWorks(schema = schema) }
            val producer = newProducer(schema) { topic(topic) }
            val consumer = newConsumer(schema) { topic(topic) }
            val message = "Hello Pulsar!"

            val receivedMessage = with(producer) { with(consumer) { message.sendAndReceive() } }

            expectThat(receivedMessage.value).isEqualTo(message)
        }
    }

    @Nested
    inner class PulsarVsKafka : PulsarTestSupport {

        override val pulsar by this@PulsarExampleTests::pulsar
        override val pulsarClient by this@PulsarExampleTests::pulsarClient
        override val pulsarAdmin by this@PulsarExampleTests::pulsarAdmin

        @Test
        fun `no partitions but messages are still consumed using the hash of the key`() = runTest(timeout = timeout) {

            val schema = Schema.STRING
            val topic = newPersistentTopic().also { it.ensureWorks(schema = schema, numberOfPartitions = 0) }

            val subscriptionName = "no-partitions-consumers"
            val consumer1 = newConsumer(schema) { topic(topic).consumerName("1").subscriptionName(subscriptionName).subscriptionType(Key_Shared) }
            val consumer2 = newConsumer(schema) { topic(topic).consumerName("2").subscriptionName(subscriptionName).subscriptionType(Key_Shared) }
            val producer = newProducer(schema) { topic(topic) }

            (1..20).forEach {
                val key = it.toString()
                val value = it.toString()
                val messageId = producer.newMessage().key(key).value(value).send()
                println("Sent partition: ${messageId.partitionIndex}, offset: ${messageId.entryId}, key: $key, value: $value")
            }

            val consumerGroup = ConsumerGroup(consumer1, consumer2)
            val receivedMessages = consumerGroup.receiveMessages(20)

            expectThat(receivedMessages).hasSize(20)
            expectThat(receivedMessages.filter { it.consumerName == consumer1.name }.map { it.message.key }).doesNotContain(receivedMessages.filter { it.consumerName == consumer2.name }.map { it.message.key })
            val consumer1MessagesOnTheSamePartition = receivedMessages.filter { it.consumerName == consumer1.name }.groupBy { (it.message.messageId as MessageIdAdv).partitionIndex }.entries.single().value
            expectThat(consumer1MessagesOnTheSamePartition).hasSize(receivedMessages.filter { it.consumerName == consumer1.name }.size)
            val consumer2MessagesOnTheSamePartition = receivedMessages.filter { it.consumerName == consumer2.name }.groupBy { (it.message.messageId as MessageIdAdv).partitionIndex }.entries.single().value
            expectThat(consumer2MessagesOnTheSamePartition).hasSize(receivedMessages.filter { it.consumerName == consumer2.name }.size)
        }
    }
}

class ConsumerGroup<T>(val consumers: Set<KotlinConsumer<T>>) : Closeable {

    constructor(vararg consumers: KotlinConsumer<T>) : this(consumers.toSet())

    init {
        require(consumers.isNotEmpty())
    }

    suspend fun receiveMessages(maxCount: Int): List<ReceivedMessage<T>> {

        val job = Job()
        val exceptionHandler = CoroutineExceptionHandler { ctx, error ->
            error.printStackTrace()
            ctx.cancel()
        }
        val scope = CoroutineScope(job + exceptionHandler)
        val received = mutableListOf<ReceivedMessage<T>>()
        consumers.forEach { consumer ->
//            withContext(Dispatchers.IO) {
            scope.launch {
                consumer.messages().onEach(consumer::acknowledge).onEach { consumer.addReceived(it, received) }.onEach {
                    println("Consumer ${consumer.name} received message ${it.info}, size is ${received.size}, ${received.size >= 20}")
                    if (received.size >= maxCount) {
                        job.cancelAndJoin()
//                            job.complete()
                    }
                }.collect()
            }
//            }
        }
        job.join()
        scope.cancel()
        return received
    }

    data class ReceivedMessage<T>(val consumerName: String, val message: Message<T>)

    private data class MessageInfo<T>(val partition: Int, val offset: Long, val key: String, val value: T) {

        constructor(id: MessageIdAdv, message: Message<T>) : this(id.partitionIndex, id.entryId, message.key, message.value)
    }

    private val <T> Message<T>.info: MessageInfo<T> get() = (messageId as MessageIdAdv).let { id -> MessageInfo(id, this) }

    override fun close() {
        consumers.forEach(KotlinConsumer<T>::close)
    }

    private fun <T> KotlinConsumer<T>.addReceived(message: Message<T>, received: MutableList<ReceivedMessage<T>>) {

        synchronized(received) {
            received.add(ReceivedMessage(name, message))
        }
    }
}
