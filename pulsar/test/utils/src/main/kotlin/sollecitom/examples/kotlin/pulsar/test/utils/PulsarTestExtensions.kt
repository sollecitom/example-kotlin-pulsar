package sollecitom.examples.kotlin.pulsar.test.utils

import kotlinx.coroutines.flow.first
import org.apache.pulsar.client.api.Message
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.KotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.producer.KotlinProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic

context(KotlinProducer<T>, KotlinConsumer<T>)
suspend fun <T> T.sendAndReceive(): Message<T> {

    require(producerTopic == consumerTopic) { "Producer and Consumer topic must be the same to use this function" }

    send(this)
    val receivedMessage = messages().first()
    acknowledge(receivedMessage)
    return receivedMessage
}

private val KotlinProducer<*>.producerTopic: PulsarTopic get() = topic
private val KotlinConsumer<*>.consumerTopic: PulsarTopic get() = topic