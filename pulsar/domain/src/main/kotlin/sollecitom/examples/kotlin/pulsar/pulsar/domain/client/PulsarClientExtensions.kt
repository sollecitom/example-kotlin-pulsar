package sollecitom.examples.kotlin.pulsar.pulsar.domain.client

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.*
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.KotlinConsumer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.KotlinConsumerAdapter
import sollecitom.examples.kotlin.pulsar.pulsar.domain.producer.KotlinProducer
import sollecitom.examples.kotlin.pulsar.pulsar.domain.producer.KotlinProducerAdapter

suspend fun <T> PulsarClient.newProducer(schema: Schema<T>, customize: ProducerBuilder<T>.() -> Unit = {}): Producer<T> = withContext(Dispatchers.VirtualThreads) {

    newProducer(schema).also(customize).create()
}

suspend fun <T> PulsarClient.newConsumer(schema: Schema<T>, customize: ConsumerBuilder<T>.() -> Unit = {}): Consumer<T> = withContext(Dispatchers.VirtualThreads) {

    newConsumer(schema).also(customize).subscribe()
}

suspend fun <T> PulsarClient.newKotlinProducer(schema: Schema<T>, customize: ProducerBuilder<T>.() -> Unit = {}): KotlinProducer<T> = newProducer(schema, customize).let(::KotlinProducerAdapter)

suspend fun <T> PulsarClient.newKotlinConsumer(schema: Schema<T>, customize: ConsumerBuilder<T>.() -> Unit = {}): KotlinConsumer<T> = newConsumer(schema, customize).let(::KotlinConsumerAdapter)
