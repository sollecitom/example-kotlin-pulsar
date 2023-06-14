package sollecitom.examples.kotlin.pulsar.pulsar.domain.producer

import org.apache.pulsar.client.api.Producer

internal class KotlinProducerAdapter<T>(override val nativeProducer: Producer<T>) : KotlinProducer<T> {

}