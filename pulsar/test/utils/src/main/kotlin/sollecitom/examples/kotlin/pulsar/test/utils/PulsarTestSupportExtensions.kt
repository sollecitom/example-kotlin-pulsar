package sollecitom.examples.kotlin.pulsar.test.utils

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.topic
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic

suspend fun <T> PulsarTestSupport.newConsumerGroup(topic: PulsarTopic, schema: Schema<T>, consumersCount: Int, subscriptionType: SubscriptionType, subscriptionName: String = randomString()): ConsumerGroup<T> {

    val consumers = (0 until consumersCount).map { newConsumer(schema, subscriptionName) { topic(topic).subscriptionType(subscriptionType).consumerName("consumer-$it") } }
    return ConsumerGroup(consumers)
}