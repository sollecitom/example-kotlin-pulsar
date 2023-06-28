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

            val producer = newProducer(schema) { topic(topic) }

            val consumerGroup = newConsumerGroup(topic, schema, 2, Key_Shared)

            (1..20).forEach {
                val key = it.toString()
                val value = it.toString()
                val messageId = producer.newMessage().key(key).value(value).send()
                println("Sent partition: ${messageId.partitionIndex}, offset: ${messageId.entryId}, key: $key, value: $value")
            }

            val receivedMessages = consumerGroup.receiveMessages(maxCount = 20)

            val consumer1 = consumerGroup.consumers.single { it.name == "consumer-1" } // TODO improve this crap
            val consumer2 = consumerGroup.consumers.single { it.name == "consumer-2" }

            expectThat(receivedMessages).hasSize(20)
            expectThat(receivedMessages.filter { it.consumerName == consumer1.name }.map { it.message.key }).doesNotContain(receivedMessages.filter { it.consumerName == consumer2.name }.map { it.message.key })
            val consumer1MessagesOnTheSamePartition = receivedMessages.filter { it.consumerName == consumer1.name }.groupBy { (it.message.messageId as MessageIdAdv).partitionIndex }.entries.single().value
            expectThat(consumer1MessagesOnTheSamePartition).hasSize(receivedMessages.filter { it.consumerName == consumer1.name }.size)
            val consumer2MessagesOnTheSamePartition = receivedMessages.filter { it.consumerName == consumer2.name }.groupBy { (it.message.messageId as MessageIdAdv).partitionIndex }.entries.single().value
            expectThat(consumer2MessagesOnTheSamePartition).hasSize(receivedMessages.filter { it.consumerName == consumer2.name }.size)
        }
    }
}

private suspend fun <T> PulsarTestSupport.newConsumerGroup(topic: PulsarTopic, schema: Schema<T>, consumersCount: Int, subscriptionType: SubscriptionType, subscriptionName: String = UUID.randomUUID().toString()): ConsumerGroup<T> {

    val consumers = (1..consumersCount).map { newConsumer(schema, subscriptionName) { topic(topic).subscriptionType(subscriptionType).consumerName("consumer-$it") } }.toSet()
    return ConsumerGroup(consumers)
}

class ConsumerGroup<T>(val consumers: Set<KotlinConsumer<T>>) : Closeable {

    constructor(vararg consumers: KotlinConsumer<T>) : this(consumers.toSet())

    init {
        require(consumers.isNotEmpty())
    }

    suspend fun receiveMessages(maxCount: Int): List<ReceivedMessage<T>> = coroutineScope {

        val received = mutableListOf<ReceivedMessage<T>>()
        val processing = Job()
        consumers.forEach { consumer ->
            launch {
                consumer.messages().onEach(consumer::acknowledge).onEach { consumer.addReceived(it, received) }.onEach {
                    println("Consumer ${consumer.name} received message ${it.info}, size is ${received.size}, ${received.size >= maxCount}")
                    if (received.size >= maxCount) {
                        processing.complete()
                    }
                }.collect()
            }
        }
        processing.join()
        coroutineContext.cancelChildren()
        received
    }

    data class ReceivedMessage<T>(val consumerName: String, val message: Message<T>)

    private data class MessageInfo<T>(val partition: Int, val offset: Long, val key: String, val value: T) {

        constructor(id: MessageIdAdv, message: Message<T>) : this(id.partitionIndex, id.entryId, message.key, message.value)
    }

    private val <T> Message<T>.info: MessageInfo<T> get() = MessageInfo((messageId as MessageIdAdv), this)

    override fun close() {
        consumers.forEach(KotlinConsumer<T>::close)
    }

    private fun <T> KotlinConsumer<T>.addReceived(message: Message<T>, received: MutableList<ReceivedMessage<T>>) = synchronized(received) {
        received.add(ReceivedMessage(name, message))
    }
}
