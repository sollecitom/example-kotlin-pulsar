package sollecitom.examples.kotlin.pulsar.pulsar.domain.topic

internal class PersistentTopic(tenant: String, namespace: String, name: String) : PulsarTopic(tenant, namespace, name, protocol) {

    companion object {
        const val protocol = "persistent"
    }
}