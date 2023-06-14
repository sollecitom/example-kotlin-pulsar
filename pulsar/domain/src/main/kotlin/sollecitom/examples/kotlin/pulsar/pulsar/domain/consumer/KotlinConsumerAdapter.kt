package sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer

import org.apache.pulsar.client.api.Consumer

internal class KotlinConsumerAdapter<T>(override val nativeConsumer: Consumer<T>) : KotlinConsumer<T> {

}