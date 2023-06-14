package sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.*
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads

suspend fun <T> PulsarClient.newProducer(schema: Schema<T>, customize: ProducerBuilder<T>.() -> Unit = {}): Producer<T> = withContext(Dispatchers.VirtualThreads) {

    newProducer(schema).also(customize).create()
}

suspend fun <T> PulsarClient.newConsumer(schema: Schema<T>, customize: ConsumerBuilder<T>.() -> Unit = {}): Consumer<T> = withContext(Dispatchers.VirtualThreads) {

    newConsumer(schema).also(customize).subscribe()
}