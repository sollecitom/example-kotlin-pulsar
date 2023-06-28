package sollecitom.examples.kotlin.pulsar.test.utils

import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer.KotlinConsumer
import java.io.Closeable

class ConsumerGroup<T>(val consumersByIndex: Map<Int, KotlinConsumer<T>>) : Closeable {

    constructor(vararg consumers: KotlinConsumer<T>) : this(consumers.toList())
    constructor(consumers: Collection<KotlinConsumer<T>>) : this(consumers.mapIndexed { index, consumer -> index to consumer }.toMap())

    init {
        require(consumersByIndex.isNotEmpty())
    }

    val consumers: Collection<KotlinConsumer<T>> get() = consumersByIndex.values

    suspend fun receiveMessages(maxCount: Int): List<ReceivedMessage<T>> = coroutineScope {

        val received = mutableListOf<ReceivedMessage<T>>()
        val processing = Job()
        consumers.forEach { consumer ->
            launch {
                consumer.messages().onEach(consumer::acknowledge).map { ReceivedMessage(consumer.name, it) }.onEach { synchronized(received) { received.add(it) } }.onEach {
                    println("Received message $it")
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
}