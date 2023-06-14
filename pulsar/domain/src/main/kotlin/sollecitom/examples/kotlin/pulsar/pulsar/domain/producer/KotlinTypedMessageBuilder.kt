package sollecitom.examples.kotlin.pulsar.pulsar.domain.producer

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.client.api.TypedMessageBuilder
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import java.util.concurrent.TimeUnit
import kotlin.time.Duration

interface KotlinTypedMessageBuilder<T> {

    val nativeBuilder: TypedMessageBuilder<T>

    /**
     * Send a message synchronously.
     *
     * <p>This method will block until the message is successfully published and returns the
     * {@link MessageId} assigned by the broker to the published message.
     *
     * <p>Example:
     *
     * <pre>{@code
     * MessageId msgId = producer.newMessage()
     *                  .key(myKey)
     *                  .value(myValue)
     *                  .send();
     * System.out.println("Published message: " + msgId);
     * }</pre>
     *
     * @return the {@link MessageId} assigned by the broker to the published message.
     */
    suspend fun send(): MessageId = withContext(Dispatchers.VirtualThreads) { nativeBuilder.send() }

    /**
     * Sets the key of the message for routing policy.
     *
     * @param key the partitioning key for the message
     * @return the message builder instance
     */
    fun key(key: String?): KotlinTypedMessageBuilder<T> = natively { it.key(key) }

    /**
     * Sets the bytes of the key of the message for routing policy.
     * Internally the bytes will be base64 encoded.
     *
     * @param key routing key for message, in byte array form
     * @return the message builder instance
     */
    fun keyBytes(key: ByteArray?): KotlinTypedMessageBuilder<T> = natively { it.keyBytes(key) }

    /**
     * Sets the ordering key of the message for message dispatch in [SubscriptionType.Key_Shared] mode.
     * Partition key will be used if ordering key not specified.
     *
     * @param orderingKey the ordering key for the message
     * @return the message builder instance
     */
    fun orderingKey(orderingKey: ByteArray): KotlinTypedMessageBuilder<T> = natively { it.orderingKey(orderingKey) }

    /**
     * Set a domain object on the message.
     *
     * @param value
     * the domain object
     * @return the message builder instance
     */
    fun value(value: T?): KotlinTypedMessageBuilder<T> = natively { it.value(value) }

    /**
     * Sets a new property on a message.
     *
     * @param name
     * the name of the property
     * @param value
     * the associated value
     * @return the message builder instance
     */
    fun property(name: String, value: String): KotlinTypedMessageBuilder<T> = natively { it.property(name, value) }

    /**
     * Add all the properties in the provided map.
     * @return the message builder instance
     */
    fun properties(properties: Map<String, String>): KotlinTypedMessageBuilder<T> = natively { it.properties(properties) }

    /**
     * Set the event time for a given message.
     *
     *
     * Applications can retrieve the event time by calling [Message.getEventTime].
     *
     *
     * Note: currently pulsar doesn't support event-time based index. so the subscribers
     * can't seek the messages by event time.
     * @return the message builder instance
     */
    fun eventTime(timestamp: Long): KotlinTypedMessageBuilder<T> = natively { it.eventTime(timestamp) }

    /**
     * Specify a custom sequence id for the message being published.
     *
     *
     * The sequence id can be used for deduplication purposes and it needs to follow these rules:
     *
     *  1. `sequenceId >= 0`
     *  1. Sequence id for a message needs to be greater than sequence id for earlier messages:
     * `sequenceId(N+1) > sequenceId(N)`
     *  1. It's not necessary for sequence ids to be consecutive. There can be holes between messages. Eg. the
     * `sequenceId` could represent an offset or a cumulative size.
     *
     *
     * @param sequenceId
     * the sequence id to assign to the current message
     * @return the message builder instance
     */
    fun sequenceId(sequenceId: Long): KotlinTypedMessageBuilder<T> = natively { it.sequenceId(sequenceId) }

    /**
     * Override the geo-replication clusters for this message.
     *
     * @param clusters the list of clusters.
     * @return the message builder instance
     */
    fun replicationClusters(clusters: List<String>): KotlinTypedMessageBuilder<T> = natively { it.replicationClusters(clusters) }

    /**
     * Disable geo-replication for this message.
     *
     * @return the message builder instance
     */
    fun disableReplication(): KotlinTypedMessageBuilder<T> = natively { it.disableReplication() }

    /**
     * Deliver the message only at or after the specified absolute timestamp.
     *
     *
     * The timestamp is milliseconds and based on UTC (eg: [System.currentTimeMillis].
     *
     *
     * **Note**: messages are only delivered with delay when a consumer is consuming
     * through a [SubscriptionType.Shared] subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param timestamp
     * absolute timestamp indicating when the message should be delivered to consumers
     * @return the message builder instance
     */
    fun deliverAt(timestamp: Long): KotlinTypedMessageBuilder<T> = natively { it.deliverAt(timestamp) }

    /**
     * Request to deliver the message only after the specified relative delay.
     *
     *
     * **Note**: messages are only delivered with delay when a consumer is consuming
     * through a [SubscriptionType.Shared] subscription. With other subscription
     * types, the messages will still be delivered immediately.
     *
     * @param delay
     * the amount of delay before the message will be delivered
     * @return the message builder instance
     */
    fun deliverAfter(delay: Duration): KotlinTypedMessageBuilder<T> = natively { it.deliverAfter(delay.inWholeMilliseconds, TimeUnit.MILLISECONDS) }

    /**
     * Configure the [TypedMessageBuilder] from a config map, as an alternative compared
     * to call the individual builder methods.
     *
     *
     * The "value" of the message itself cannot be set on the config map.
     *
     *
     * Example:
     *
     * <pre>`Map<String, Object> conf = new HashMap<>();
     * conf.put("key", "my-key");
     * conf.put("eventTime", System.currentTimeMillis());
     *
     * producer.newMessage()
     * .value("my-message")
     * .loadConf(conf)
     * .send();
    `</pre> *
     *
     *
     * The available options are:
     * <table border="1">
     * <tr>
     * <th>Constant</th>
     * <th>Name</th>
     * <th>Type</th>
     * <th>Doc</th>
    </tr> *
     * <tr>
     * <td>[.CONF_KEY]</td>
     * <td>`key`</td>
     * <td>`String`</td>
     * <td>[.key]</td>
    </tr> *
     * <tr>
     * <td>[.CONF_PROPERTIES]</td>
     * <td>`properties`</td>
     * <td>`Map<String,String>`</td>
     * <td>[.properties]</td>
    </tr> *
     * <tr>
     * <td>[.CONF_EVENT_TIME]</td>
     * <td>`eventTime`</td>
     * <td>`long`</td>
     * <td>[.eventTime]</td>
    </tr> *
     * <tr>
     * <td>[.CONF_SEQUENCE_ID]</td>
     * <td>`sequenceId`</td>
     * <td>`long`</td>
     * <td>[.sequenceId]</td>
    </tr> *
     * <tr>
     * <td>[.CONF_REPLICATION_CLUSTERS]</td>
     * <td>`replicationClusters`</td>
     * <td>`List<String>`</td>
     * <td>[.replicationClusters]</td>
    </tr> *
     * <tr>
     * <td>[.CONF_DISABLE_REPLICATION]</td>
     * <td>`disableReplication`</td>
     * <td>`boolean`</td>
     * <td>[.disableReplication]</td>
    </tr> *
     * <tr>
     * <td>[.CONF_DELIVERY_AFTER_SECONDS]</td>
     * <td>`deliverAfterSeconds`</td>
     * <td>`long`</td>
     * <td>[.deliverAfter]</td>
    </tr> *
     * <tr>
     * <td>[.CONF_DELIVERY_AT]</td>
     * <td>`deliverAt`</td>
     * <td>`long`</td>
     * <td>[.deliverAt]</td>
    </tr> *
    </table> *
     *
     * @param config a map with the configuration options for the message
     * @return the message builder instance
     */
    fun loadConf(config: Map<String, Any>): KotlinTypedMessageBuilder<T> = natively { it.loadConf(config) }

    private fun natively(action: (TypedMessageBuilder<T>) -> Unit): KotlinTypedMessageBuilder<T> = action(nativeBuilder).let { this }
}

internal data class KotlinTypedMessageBuilderAdapter<T>(override val nativeBuilder: TypedMessageBuilder<T>) : KotlinTypedMessageBuilder<T>