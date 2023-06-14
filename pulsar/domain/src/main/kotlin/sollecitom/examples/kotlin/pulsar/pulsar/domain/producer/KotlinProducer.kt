package sollecitom.examples.kotlin.pulsar.pulsar.domain.producer

import org.apache.pulsar.client.api.Producer

interface KotlinProducer<T> {

    val nativeProducer: Producer<T>
}