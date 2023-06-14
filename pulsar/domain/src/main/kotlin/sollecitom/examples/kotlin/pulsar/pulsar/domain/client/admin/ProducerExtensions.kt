package sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.ProducerBuilder
import org.apache.pulsar.client.api.TypedMessageBuilder
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic

fun <T> ProducerBuilder<T>.topic(topic: PulsarTopic): ProducerBuilder<T> = topic(topic.fullName)

suspend fun <T> Producer<T>.sendSuspending(message: T): MessageId = withContext(Dispatchers.VirtualThreads) { send(message) }

suspend fun <T> Producer<T>.closeSuspending(): Unit = withContext(Dispatchers.VirtualThreads) { close() }

suspend fun <T> Producer<T>.flushSuspending(): Unit = withContext(Dispatchers.VirtualThreads) { flush() }

suspend fun <T> TypedMessageBuilder<T>.sendSuspending(): MessageId = withContext(Dispatchers.VirtualThreads) { send() }

