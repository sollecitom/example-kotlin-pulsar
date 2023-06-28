package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.*
import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.await
import kotlinx.coroutines.test.runTest
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.api.SubscriptionType.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin.*
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.client.newKotlinProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.topic
import sollecitom.examples.kotlin.pulsar.pulsar.domain.message.partitionIndex
import sollecitom.examples.kotlin.pulsar.pulsar.domain.producer.*
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import sollecitom.examples.kotlin.pulsar.test.utils.*
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.doesNotContain
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNotEqualTo
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
            val consumersCount = 2
            val consumerGroup = newConsumerGroup(consumersCount = consumersCount, subscriptionType = Key_Shared, topic = topic, schema = schema)
            val messageCountPerKey = 2
            val keysCount = 10
            val expectedMessageCount = messageCountPerKey * keysCount

            producer.sendTestMessages(messageCountPerKey, keysCount)
            val receivedMessages = consumerGroup.receiveMessages(maxCount = expectedMessageCount)
            val receivedMessagesByConsumer = receivedMessages.groupBy { it.consumerName }

            expectThat(receivedMessages).hasSize(expectedMessageCount)
            expectThat(receivedMessagesByConsumer).hasSize(consumersCount)
            expectThat(receivedMessagesByConsumer).doesNotContainTheSameKeyAcrossMultipleConsumers()
        }

        @Test
        fun `multiple partitions but messages are still consumed in round-robin`() = runTest(timeout = timeout) {

            val schema = Schema.STRING
            val topic = newPersistentTopic().also { it.ensureWorks(schema = schema, numberOfPartitions = 2) }
            val producer = newProducer(schema) { topic(topic) }
            val consumersCount = 2
            val consumerGroup = newConsumerGroup(consumersCount = consumersCount, subscriptionType = Shared, topic = topic, schema = schema)
            val messageCountPerKey = 2
            val keysCount = 20
            val expectedMessageCount = messageCountPerKey * keysCount

            producer.sendTestMessages(messageCountPerKey, keysCount)
            val receivedMessages = consumerGroup.receiveMessages(maxCount = expectedMessageCount)
            val receivedMessagesByConsumer = receivedMessages.groupBy { it.consumerName }

            expectThat(receivedMessages).hasSize(expectedMessageCount)
            expectThat(receivedMessagesByConsumer).hasSize(consumersCount)
            expectThat(receivedMessagesByConsumer).containsMessagesAcrossMultiplePartitionsForEachConsumer()
        }

        @Test
        fun `partitions and regular partitioned message consumption`() = runTest(timeout = timeout) {

            val schema = Schema.STRING
            val topic = newPersistentTopic().also { it.ensureWorks(schema = schema, numberOfPartitions = 4) }
            val producer = newProducer(schema) { topic(topic) }
            val consumersCount = 2
            val consumerGroup = newConsumerGroup(consumersCount = consumersCount, subscriptionType = Failover, topic = topic, schema = schema)
            val messageCountPerKey = 2
            val keysCount = 10
            val expectedMessageCount = messageCountPerKey * keysCount

            producer.sendTestMessages(messageCountPerKey, keysCount)
            val receivedMessages = consumerGroup.receiveMessages(maxCount = expectedMessageCount)
            val receivedMessagesByConsumer = receivedMessages.groupBy { it.consumerName }

            expectThat(receivedMessages).hasSize(expectedMessageCount)
            expectThat(receivedMessagesByConsumer).hasSize(consumersCount)
            expectThat(receivedMessagesByConsumer).doesNotContainTheSameKeyAcrossMultipleConsumers()
            expectThat(receivedMessagesByConsumer).containsMessagesAcrossMultiplePartitionsForEachConsumer()
        }
    }
}

private suspend fun KotlinProducer<String>.sendTestMessages(messageCountPerKey: Int, keysCount: Int) {

    (1..messageCountPerKey).forEach { messagePerKeyIndex ->
        (1..keysCount).forEach { keyIndex ->
            val key = keyIndex.toString()
            val value = (messagePerKeyIndex * keyIndex).toString()
            val messageId = newMessage().key(key).value(value).send()
            println("Sent message (partitionIndex: ${messageId.partitionIndex}, entryId: ${messageId.entryId}, key: $key, value: $value)")
        }
    }
}

private fun <T> Assertion.Builder<Map<String, List<ReceivedMessage<T>>>>.doesNotContainTheSameKeyAcrossMultipleConsumers() = compose("does not contain the same key across multiple consumers") { subject ->

    subject.forEach { (consumerName, consumerMessages) ->
        get("The received message keys for consumer $consumerName: %s") { consumerMessages.map { it.message.key } }.doesNotContainAKeyReceivedByOtherConsumers(subject - consumerName)
    }
} then {
    if (allPassed) pass() else fail()
}

private fun <T> Assertion.Builder<List<String>>.doesNotContainAKeyReceivedByOtherConsumers(otherConsumers: Map<String, List<ReceivedMessage<T>>>) = compose("does not contain the same key received by other consumers") { subject ->

    otherConsumers.forEach { otherConsumer ->
        get("Other consumer ${otherConsumer.key}: %s") { otherConsumer.value.map { it.message.key } }.doesNotContain(subject)
    }
} then {
    if (allPassed) pass() else fail()
}

private fun <T> Assertion.Builder<Map<String, List<ReceivedMessage<T>>>>.containsMessagesAcrossMultiplePartitionsForEachConsumer() = compose("contains messages from multiple partitions for each consumer") { subject ->

    subject.forEach { (consumerName, consumerMessages) ->
        consumerMessages.groupBy { it.message.partitionIndex }.forEach { (partitionIndex, partitionMessages) ->
            get("The number of received messages for consumer '$consumerName' on partition '$partitionIndex' %s") { partitionMessages.size }.isNotEqualTo(consumerMessages.size)
        }
    }
} then {
    if (allPassed) pass() else fail()
}