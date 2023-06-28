package sollecitom.examples.kotlin.pulsar.pulsar.domain.message

import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.MessageIdAdv

val Message<*>.id: MessageIdAdv get() = messageId as MessageIdAdv

val Message<*>.partitionIndex: Int get() = id.partitionIndex
val Message<*>.entryId: Long get() = id.entryId