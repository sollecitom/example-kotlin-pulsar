package sollecitom.examples.kotlin.pulsar.messaging.domain.topic

interface Topic {

    val tenant: String
    val namespace: String
    val protocol: String
    val name: String

    val tenantAndNamespace: String
    val fullName: String
}