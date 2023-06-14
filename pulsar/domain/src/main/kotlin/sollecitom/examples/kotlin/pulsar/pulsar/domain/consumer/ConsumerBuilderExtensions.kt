package sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.ConsumerBuilder
import org.apache.pulsar.client.api.Message
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic

fun <T> ConsumerBuilder<T>.topic(topic: PulsarTopic): ConsumerBuilder<T> = topic(topic.fullName)

// TODO remove this
val <T> Consumer<T>.messages: Flow<Message<T>>
    get() = flow {
        while (currentCoroutineContext().isActive) {
            val message = receiveSuspending()
            emit(message)
        }
    }

// TODO remove this
private suspend fun <T> Consumer<T>.receiveSuspending(): Message<T> = withContext(Dispatchers.VirtualThreads) { receive() }