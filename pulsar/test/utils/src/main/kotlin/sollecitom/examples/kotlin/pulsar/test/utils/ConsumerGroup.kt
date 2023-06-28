package sollecitom.examples.kotlin.pulsar.test.utils

import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import org.apache.pulsar.client.api.Message
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.KotlinConsumer
import java.io.Closeable

class ConsumerGroup<T>(val consumersByIndex: Map<Int, KotlinConsumer<T>>, private val logReceived: (ReceivedMessage<T>) -> Unit = { println("Received message $it") }) : Closeable {

    constructor(vararg consumers: KotlinConsumer<T>, logReceived: (ReceivedMessage<T>) -> Unit = { println("Received message $it") }) : this(consumers.toList(), logReceived)
    constructor(consumers: Collection<KotlinConsumer<T>>, logReceived: (ReceivedMessage<T>) -> Unit = { println("Received message $it") }) : this(consumers.mapIndexed { index, consumer -> index to consumer }.toMap(), logReceived)

    init {
        require(consumersByIndex.isNotEmpty())
    }

    val consumers: Collection<KotlinConsumer<T>> get() = consumersByIndex.values

    suspend fun receiveMessages(maxCount: Int): List<ReceivedMessage<T>> = coroutineScope {

        val received = mutableListOf<ReceivedMessage<T>>()
        val processing = Job()
        consumers.forEach { consumer ->
            launch {
                consumer.messages().onEach(consumer::acknowledge).map { it.receivedBy(consumer) }.onEach(received::addSynchronized).onEach(logReceived).onEach {
                    if (received.size >= maxCount) {
                        processing.complete()
                    }
                }.collect()
            }
        }
        processing.join()
        coroutineContext.cancelChildren()
        received
    }

    override fun close() = consumers.forEach(KotlinConsumer<T>::close)

    private fun <T> Message<T>.receivedBy(consumer: KotlinConsumer<T>) = ReceivedMessage(consumer.name, this)
}

private fun <E> MutableList<E>.addSynchronized(element: E) = synchronized(this) { add(element) }