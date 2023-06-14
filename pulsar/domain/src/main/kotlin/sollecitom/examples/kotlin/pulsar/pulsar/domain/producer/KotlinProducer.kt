package sollecitom.examples.kotlin.pulsar.pulsar.domain.producer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.api.transaction.Transaction
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.runInVirtualThreads
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import java.io.Closeable

interface KotlinProducer<T> : Closeable {

    val nativeProducer: Producer<T>

    override fun close() = runInVirtualThreads { nativeProducer.close() }

    /**
     * @return the topic which producer is publishing to
     */
    val topic: PulsarTopic get() = PulsarTopic.parse(nativeProducer.topic)

    /**
     * @return the producer name which could have been assigned by the system or specified by the client
     */
    val name: String get() = nativeProducer.producerName

    /**
     * Get the last sequence id that was published by this producer.
     *
     * <p>This represent either the automatically assigned
     * or custom sequence id (set on the {@link TypedMessageBuilder})
     * that was published and acknowledged by the broker.
     *
     * <p>After recreating a producer with the same producer name, this will return the last message that was
     * published in the previous producer session, or -1 if there no message was ever published.
     *
     * @return the last sequence id published by this producer
     */
    val lastSequenceId: Long get() = nativeProducer.lastSequenceId

    /**
     * Get statistics for the producer.
     * <ul>
     * <li>numMsgsSent : Number of messages sent in the current interval
     * <li>numBytesSent : Number of bytes sent in the current interval
     * <li>numSendFailed : Number of messages failed to send in the current interval
     * <li>numAcksReceived : Number of acks received in the current interval
     * <li>totalMsgsSent : Total number of messages sent
     * <li>totalBytesSent : Total number of bytes sent
     * <li>totalSendFailed : Total number of messages failed to send
     * <li>totalAcksReceived: Total number of acks received
     * </ul>
     *
     * @return statistic for the producer or null if ProducerStatsRecorderImpl is disabled.
     */
    val stats: ProducerStats get() = nativeProducer.stats

    /**
     * @return Whether the producer is currently connected to the broker
     */
    val isConnected: Boolean get() = nativeProducer.isConnected

    /**
     * @return The last disconnected timestamp of the producer
     */
    val lastDisconnectedTimestamp: Long get() = nativeProducer.lastDisconnectedTimestamp

    /**
     * @return the number of partitions per topic.
     */
    val numberOfPartitions: Int get() = nativeProducer.numOfPartitions

    /**
     * Create a new message builder.
     *
     * <p>This message builder allows to specify additional properties on the message. For example:
     * <pre>{@code
     * producer.newMessage()
     *       .key(messageKey)
     *       .value(myValue)
     *       .property("user-defined-property", "value")
     *       .send();
     * }</pre>
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     */
    fun newMessage(): KotlinTypedMessageBuilder<T> = nativeProducer.newMessage().let(::KotlinTypedMessageBuilderAdapter)

    /**
     * Create a new message builder with schema, not required same parameterized type with the producer.
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     * @see #newMessage()
     */
    fun <V> KotlinProducer<*>.newMessage(schema: Schema<V>): KotlinTypedMessageBuilder<V> = nativeProducer.newMessage(schema).let(::KotlinTypedMessageBuilderAdapter)

    /**
     * Create a new message builder with transaction.
     *
     * <p>After the transaction commit, it will be made visible to consumer.
     *
     * <p>After the transaction abort, it will never be visible to consumer.
     *
     * @return a typed message builder that can be used to construct the message to be sent through this producer
     * @see #newMessage()
     *
     * @since 2.7.0
     */
    fun newMessage(txn: Transaction): KotlinTypedMessageBuilder<T> = nativeProducer.newMessage(txn).let(::KotlinTypedMessageBuilderAdapter)

    /**
     * Sends a message.
     *
     * <p>This call will be blocking until is successfully acknowledged by the Pulsar broker.
     *
     * <p>Use {@link #newMessage()} to specify more properties than just the value on the message to be sent.
     *
     * @param message
     *            a message
     * @return the message id assigned to the published message
     * @throws PulsarClientException.TimeoutException
     *             if the message was not correctly received by the system within the timeout period
     * @throws PulsarClientException.AlreadyClosedException
     *             if the producer was already closed
     */
    suspend fun send(message: T): MessageId = withContext(Dispatchers.VirtualThreads) { nativeProducer.send(message) }
}

internal data class KotlinProducerAdapter<T>(override val nativeProducer: Producer<T>) : KotlinProducer<T>