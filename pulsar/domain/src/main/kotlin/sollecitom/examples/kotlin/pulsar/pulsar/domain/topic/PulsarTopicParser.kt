package sollecitom.examples.kotlin.pulsar.pulsar.domain.topic

import replaceFrom

internal object PulsarTopicParser {

    private const val delimiter = "-partition-"

    fun stripPartitionInfo(topic: String): String = topic.replaceFrom(delimiter, "")
}