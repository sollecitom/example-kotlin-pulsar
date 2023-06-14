package sollecitom.examples.kotlin.pulsar.pulsar.domain.topic

import sollecitom.examples.kotlin.pulsar.messaging.domain.topic.Topic
import java.util.regex.Pattern

sealed class PulsarTopic(final override val tenant: String, final override val namespace: String, name: String, final override val protocol: String) : Topic {

    final override val name = PulsarTopicParser.stripPartitionInfo(name)
    final override val tenantAndNamespace = "$tenant/$namespace"
    final override val fullName = "$protocol://$tenantAndNamespace/${this.name}"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as PulsarTopic
        if (tenant != other.tenant) return false
        if (namespace != other.namespace) return false
        if (protocol != other.protocol) return false
        return true
    }

    override fun hashCode(): Int {
        var result = tenant.hashCode()
        result = 31 * result + namespace.hashCode()
        result = 31 * result + protocol.hashCode()
        return result
    }

    override fun toString() = "${this::class.simpleName}(tenant='$tenant', namespace='$namespace', protocol='$protocol', topicName='$name', tenantAndNamespace='$tenantAndNamespace', fullTopicName='$fullName')"

    companion object {

        private const val topicPartsSeparator = "/"
        private const val maximumAllowedParts = 5
        private const val protocolPattern = "(persistent|non-persistent)"
        private const val tenantPattern = "([a-zA-Z0-9\\-]+)"
        private const val namespacePattern = "([a-zA-Z0-9\\-]+)"
        private const val topicNamePattern = "([a-zA-Z0-9\\-]+)"
        private const val pattern = "$protocolPattern://$tenantPattern/$namespacePattern/$topicNamePattern"
        private val compiled by lazy { Pattern.compile(pattern) }

        fun parse(rawTopic: String): PulsarTopic {

            require(rawTopic.split(topicPartsSeparator).size <= maximumAllowedParts)
            val matcher = compiled.matcher(rawTopic)
            if (!matcher.find()) {
                error("Invalid raw topic format in $rawTopic does not match pattern $pattern")
            }
            val protocol = matcher.group(1)
            val tenant = matcher.group(2)
            val namespace = matcher.group(3)
            val topicName = matcher.group(4)
            return create(protocol, tenant, namespace, topicName)
        }

        fun create(protocol: String, tenant: String, namespace: String, name: String): PulsarTopic = when (protocol) {
            PersistentTopic.protocol -> persistent(tenant, namespace, name)
            NonPersistentTopic.protocol -> nonPersistent(tenant, namespace, name)
            else -> error("Unknown topic protocol $protocol")
        }

        fun persistent(tenant: String, namespace: String, name: String): PulsarTopic = PersistentTopic(tenant, namespace, name)

        fun nonPersistent(tenant: String, namespace: String, name: String): PulsarTopic = NonPersistentTopic(tenant, namespace, name)
    }
}
