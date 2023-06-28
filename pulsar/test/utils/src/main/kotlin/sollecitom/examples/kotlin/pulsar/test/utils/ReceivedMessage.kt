package sollecitom.examples.kotlin.pulsar.test.utils

import org.apache.pulsar.client.api.Message
import sollecitom.examples.kotlin.pulsar.pulsar.domain.message.entryId
import sollecitom.examples.kotlin.pulsar.pulsar.domain.message.partitionIndex

data class ReceivedMessage<T>(val consumerName: String, val message: Message<T>) {

    override fun toString() = "(consumerName: ${consumerName}, partitionIndex: ${message.partitionIndex}, entryId: ${message.entryId}, key: ${message.key}, value: ${message.value})"
}