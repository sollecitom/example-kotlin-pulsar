package sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer

import org.apache.pulsar.client.api.Consumer

interface KotlinConsumer<T> {

    val nativeConsumer: Consumer<T>
}