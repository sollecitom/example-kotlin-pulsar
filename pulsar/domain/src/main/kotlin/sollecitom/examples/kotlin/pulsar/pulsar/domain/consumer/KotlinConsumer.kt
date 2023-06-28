package sollecitom.examples.kotlin.pulsar.pulsar.domain.consumer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.*
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.await
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.runInVirtualThreads
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic
import java.io.Closeable

interface KotlinConsumer<T> : Closeable {

    val nativeConsumer: Consumer<T>

    /**
     * Produces a [Flow] with all the messages the consumer receives.
     *
     * @return a [Flow] of received messages.
     */
    fun messages(): Flow<Message<T>>

    /**
     * Get a topic for the consumer.
     *
     * @return topic for the consumer
     */
    val topic: PulsarTopic get() = PulsarTopic.parse(nativeConsumer.topic)

    /**
     * Get a subscription for the consumer.
     *
     * @return subscription for the consumer
     */
    val subscriptionName: String get() = nativeConsumer.subscription

    /**
     * Unsubscribe the consumer.
     *
     *
     * This call blocks until the consumer is unsubscribed.
     *
     *
     * Unsubscribing will the subscription to be deleted and all the
     * data retained can potentially be deleted as well.
     *
     *
     * The operation will fail when performed on a shared subscription
     * where multiple consumers are currently connected.
     *
     * @throws PulsarClientException if the operation fails
     */
    suspend fun unsubscribe() = nativeConsumer.unsubscribe()

    /**
     * Receives a single message.
     *
     *
     * This calls blocks until a message is available.
     *
     *
     * When thread is Interrupted, return a null value and reset interrupted flag.
     *
     * @return the received message
     * @throws PulsarClientException.AlreadyClosedException
     * if the consumer was already closed
     * @throws PulsarClientException.InvalidConfigurationException
     * if a message listener was defined in the configuration
     */
    suspend fun receive(): Message<T> = nativeConsumer.receiveAsync().await()

    /**
     * Batch receiving messages.
     *
     *
     * This calls blocks until has enough messages or wait timeout, more details to see [BatchReceivePolicy].
     *
     * @return messages
     * @since 2.4.1
     */
    suspend fun batchReceive(): Messages<T> = nativeConsumer.batchReceiveAsync().await()

    /**
     * Acknowledge the consumption of a single message.
     *
     * @param messageId {@link MessageId} to be individual acknowledged
     *
     * @throws PulsarClientException.AlreadyClosedException}
     *             if the consumer was already closed
     * @throws PulsarClientException.NotAllowedException
     *             if `messageId` is not a {@link TopicMessageId} when multiple topics are subscribed
     */
    suspend fun acknowledge(messageId: MessageId) = nativeConsumer.acknowledgeAsync(messageId).await()

    suspend fun acknowledge(message: Message<*>) = nativeConsumer.acknowledgeAsync(message).await()

    /**
     * Acknowledge the consumption of a list of message.
     * @param messageIdList the list of message IDs.
     * @throws PulsarClientException.NotAllowedException
     *     if any message id in the list is not a {@link TopicMessageId} when multiple topics are subscribed
     */
    suspend fun acknowledge(messageIdList: List<MessageId>) = nativeConsumer.acknowledgeAsync(messageIdList).await()

    suspend fun acknowledge(messages: Messages<*>) = nativeConsumer.acknowledgeAsync(messages).await()

    /**
     * Acknowledge the failure to process a single message.
     *
     *
     * When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with [ConsumerBuilder.negativeAckRedeliveryDelay].
     *
     *
     * This call is not blocking.
     *
     *
     * Example of usage:
     * <pre>`
     * while (true) {
     * Message<String> msg = consumer.receive();
     *
     * try {
     * // Process message...
     *
     * consumer.acknowledge(msg);
     * } catch (Throwable t) {
     * log.warn("Failed to process message");
     * consumer.negativeAcknowledge(msg);
     * }
     * }
    `</pre> *
     *
     * @param message
     * The `Message` to be acknowledged
     */
    suspend fun negativeAcknowledge(message: Message<*>) = withContext(Dispatchers.VirtualThreads) { nativeConsumer.negativeAcknowledge(message) }

    /**
     * Acknowledge the failure to process a single message.
     *
     *
     * When a message is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with [ConsumerBuilder.negativeAckRedeliveryDelay].
     *
     *
     * This call is not blocking.
     *
     *
     * This variation allows to pass a [MessageId] rather than a [Message]
     * object, in order to avoid keeping the payload in memory for extended amount
     * of time
     *
     * @see .negativeAcknowledge
     * @param messageId
     * The `MessageId` to be acknowledged
     */
    suspend fun negativeAcknowledge(messageId: MessageId) = withContext(Dispatchers.VirtualThreads) { nativeConsumer.negativeAcknowledge(messageId) }

    /**
     * Acknowledge the failure to process [Messages].
     *
     *
     * When messages is "negatively acked" it will be marked for redelivery after
     * some fixed delay. The delay is configurable when constructing the consumer
     * with [ConsumerBuilder.negativeAckRedeliveryDelay].
     *
     *
     * This call is not blocking.
     *
     *
     * Example of usage:
     * <pre>`
     * while (true) {
     * Messages<String> msgs = consumer.batchReceive();
     *
     * try {
     * // Process message...
     *
     * consumer.acknowledge(msgs);
     * } catch (Throwable t) {
     * log.warn("Failed to process message");
     * consumer.negativeAcknowledge(msgs);
     * }
     * }
    `</pre> *
     *
     * @param messages
     * The `Message` to be acknowledged
     */
    suspend fun negativeAcknowledge(messages: Messages<*>) = withContext(Dispatchers.VirtualThreads) { nativeConsumer.negativeAcknowledge(messages) }

    /**
     * Get statistics for the consumer.
     *
     *  * numMsgsReceived : Number of messages received in the current interval
     *  * numBytesReceived : Number of bytes received in the current interval
     *  * numReceiveFailed : Number of messages failed to receive in the current interval
     *  * numAcksSent : Number of acks sent in the current interval
     *  * numAcksFailed : Number of acks failed to send in the current interval
     *  * totalMsgsReceived : Total number of messages received
     *  * totalBytesReceived : Total number of bytes received
     *  * totalReceiveFailed : Total number of messages failed to receive
     *  * totalAcksSent : Total number of acks sent
     *  * totalAcksFailed : Total number of acks failed to sent
     *
     *
     * @return statistic for the consumer
     */
    val stats: ConsumerStats get() = nativeConsumer.stats

    /**
     * Close the consumer and stop the broker to push more messages.
     */
    override fun close() = runInVirtualThreads { nativeConsumer.close() }

    /**
     * Return true if the topic was terminated and this consumer has already consumed all the messages in the topic.
     *
     *
     * Please note that this does not simply mean that the consumer is caught up with the last message published by
     * producers, rather the topic needs to be explicitly "terminated".
     */
    fun hasReachedEndOfTopic(): Boolean = nativeConsumer.hasReachedEndOfTopic()

    /**
     * Reset the subscription associated with this consumer to a specific message id.
     *
     *
     * The message id can either be a specific message or represent the first or last messages in the topic.
     *
     *  * `MessageId.earliest` : Reset the subscription on the earliest message available in the topic
     *  * `MessageId.latest` : Reset the subscription on the latest message in the topic
     *
     *
     *
     * Note: For multi-topics consumer, if `messageId` is a [TopicMessageId], the seek operation will happen
     * on the owner topic of the message, which is returned by [TopicMessageId.getOwnerTopic]. Otherwise, you
     * can only seek to the earliest or latest message for all topics subscribed.
     *
     * @param messageId
     * the message id where to reposition the subscription
     */
    suspend fun seek(messageId: MessageId) = nativeConsumer.seekAsync(messageId).await()

    /**
     * Reset the subscription associated with this consumer to a specific message publish time.
     *
     * @param timestamp
     * the message publish time where to reposition the subscription
     * The timestamp format should be Unix time in milliseconds.
     */
    suspend fun seek(timestamp: Long) = nativeConsumer.seekAsync(timestamp).await()

    /**
     * Reset the subscription associated with this consumer to a specific message ID or message publish time.
     *
     *
     * The Function input is topic+partition. It returns only timestamp or MessageId.
     *
     *
     * The return value is the seek position/timestamp of the current partition.
     * Exception is thrown if other object types are returned.
     *
     *
     * If returns null, the current partition will not do any processing.
     * Exception in a partition may affect other partitions.
     * @param function
     * @throws PulsarClientException
     */
    suspend fun seek(function: (String) -> Any) = nativeConsumer.seekAsync(function).await()

    /**
     * @return Whether the consumer is connected to the broker
     */
    val isConnected: Boolean get() = nativeConsumer.isConnected

    /**
     * Get the name of consumer.
     * @return consumer name.
     */
    val name: String get() = nativeConsumer.consumerName

    /**
     * Stop requesting new messages from the broker until [.resume] is called. Note that this might cause
     * [.receive] to block until [.resume] is called and new messages are pushed by the broker.
     */
    suspend fun pause() = withContext(Dispatchers.VirtualThreads) { nativeConsumer.pause() }

    /**
     * Resume requesting messages from the broker.
     */
    suspend fun resume() = withContext(Dispatchers.VirtualThreads) { nativeConsumer.resume() }
}

internal data class KotlinConsumerAdapter<T>(override val nativeConsumer: Consumer<T>) : KotlinConsumer<T> {

    override fun messages(): Flow<Message<T>> = flow {

        while (currentCoroutineContext().isActive) {
            val message = receive()
            emit(message)
        }
    }
}