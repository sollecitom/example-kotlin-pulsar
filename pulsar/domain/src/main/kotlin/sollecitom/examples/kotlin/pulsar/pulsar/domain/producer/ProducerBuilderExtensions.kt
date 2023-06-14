package sollecitom.examples.kotlin.pulsar.pulsar.domain.producer

import org.apache.pulsar.client.api.ProducerBuilder
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic

fun <T> ProducerBuilder<T>.topic(topic: PulsarTopic): ProducerBuilder<T> = topic(topic.fullName)

