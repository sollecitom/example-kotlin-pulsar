package sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.*
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.await
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

fun <T> ConsumerBuilder<T>.topic(topic: PulsarTopic): ConsumerBuilder<T> = topic(topic.fullName)

val <T> Consumer<T>.messages: Flow<Message<T>>
    get() = flow {
        while (currentCoroutineContext().isActive) {
            val message = receiveSuspending()
            emit(message)
        }
    }

suspend fun <T> Consumer<T>.receiveSuspending(): Message<T> = withContext(Dispatchers.VirtualThreads) { receive() }

suspend fun <T> Consumer<T>.acknowledgeSuspending(message: Message<T>) = withContext(Dispatchers.VirtualThreads) { acknowledge(message) }
suspend fun <T> Consumer<T>.acknowledgeSuspending(messageId: MessageId) = withContext(Dispatchers.VirtualThreads) { acknowledge(messageId) }
suspend fun <T> Consumer<T>.acknowledgeSuspending(messages: Messages<*>) = withContext(Dispatchers.VirtualThreads) { acknowledge(messages) }
suspend fun <T> Consumer<T>.acknowledgeSuspending(messageIdList: List<MessageId>) = withContext(Dispatchers.VirtualThreads) { acknowledge(messageIdList) }

suspend fun <T> Consumer<T>.closeSuspending(): Unit = withContext(Dispatchers.VirtualThreads) { close() }

suspend fun <T> Consumer<T>.batchReceiveSuspending(): Unit = withContext(Dispatchers.VirtualThreads) { batchReceive() }

suspend fun <T> Consumer<T>.reconsumeLaterSuspending(message: Message<T>, delay: Duration, customProperties: Map<String, String> = emptyMap()): Unit = withContext(Dispatchers.VirtualThreads) { reconsumeLater(message, customProperties, delay.inWholeMilliseconds, TimeUnit.MILLISECONDS) }
suspend fun <T> Consumer<T>.reconsumeLaterSuspending(messages: Messages<T>, delay: Duration): Unit = withContext(Dispatchers.VirtualThreads) { reconsumeLater(messages, delay.inWholeMilliseconds, TimeUnit.MILLISECONDS) }

suspend fun <T> Consumer<T>.reconsumeLaterCumulativeAsync(message: Message<T>, delay: Duration, customProperties: Map<String, String> = emptyMap()): Unit = withContext(Dispatchers.VirtualThreads) { reconsumeLaterCumulativeAsync(message, customProperties, delay.inWholeMilliseconds, TimeUnit.MILLISECONDS).await() }

suspend fun <T> Consumer<T>.lastMessageIdsSuspending(): Unit = withContext(Dispatchers.VirtualThreads) { lastMessageIds }

suspend fun <T> Consumer<T>.seekSuspending(timestamp: Long): Unit = withContext(Dispatchers.VirtualThreads) { seek(timestamp) }
suspend fun <T> Consumer<T>.seekSuspending(messageId: MessageId): Unit = withContext(Dispatchers.VirtualThreads) { seek(messageId) }
suspend fun <T> Consumer<T>.seekSuspending(function: (String) -> Any): Unit = withContext(Dispatchers.VirtualThreads) { seek(function) }

suspend fun <T> Consumer<T>.unsubscribeSuspending(): Unit = withContext(Dispatchers.VirtualThreads) { unsubscribe() }

suspend fun <T> Consumer<T>.acknowledgeCumulativeSuspending(message: Message<T>) = withContext(Dispatchers.VirtualThreads) { acknowledgeCumulative(message) }
suspend fun <T> Consumer<T>.acknowledgeCumulativeSuspending(messageId: MessageId) = withContext(Dispatchers.VirtualThreads) { acknowledgeCumulative(messageId) }