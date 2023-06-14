package sollecitom.examples.kotlin.pulsar.pulsar.domain.client.admin

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.*
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy
import org.apache.pulsar.common.policies.data.TenantInfo
import org.apache.pulsar.common.policies.data.TopicType
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.VirtualThreads
import sollecitom.examples.kotlin.pulsar.kotlin.extensions.runInVirtualThreads
import sollecitom.examples.kotlin.pulsar.pulsar.domain.topic.PulsarTopic

suspend fun PulsarAdmin.createTopic(fullyQualifiedTopic: String, numberOfPartitions: Int = 1) = withContext(Dispatchers.VirtualThreads) { topics().createPartitionedTopic(fullyQualifiedTopic, numberOfPartitions) }

suspend fun PulsarAdmin.createTenant(tenant: String) = withContext(Dispatchers.VirtualThreads) {

    val allClusters = clusters().clusters.toSet()
    tenants().createTenant(tenant, TenantInfo.builder().allowedClusters(allClusters).build())
}

suspend fun PulsarAdmin.createNamespace(tenant: String, namespace: String) = withContext(Dispatchers.VirtualThreads) { namespaces().createNamespace("$tenant/$namespace") }

suspend fun PulsarAdmin.configureNamespace(tenant: String, namespace: String, allowTopicCreation: Boolean = false, isAllowAutoUpdateSchema: Boolean = false, schemaValidationEnforced: Boolean = true, schemaCompatibilityStrategy: SchemaCompatibilityStrategy = SchemaCompatibilityStrategy.FULL_TRANSITIVE) = withContext(Dispatchers.VirtualThreads) {

    val tenantNamespace = "$tenant/$namespace"
    namespaces().setAutoTopicCreation(tenantNamespace, AutoTopicCreationOverride.builder().allowAutoTopicCreation(allowTopicCreation).topicType(TopicType.PARTITIONED.name).build())
    namespaces().setAutoTopicCreation(tenantNamespace, AutoTopicCreationOverride.builder().allowAutoTopicCreation(allowTopicCreation).topicType(TopicType.NON_PARTITIONED.name).build())
    namespaces().setIsAllowAutoUpdateSchema(tenantNamespace, isAllowAutoUpdateSchema)
    namespaces().setSchemaValidationEnforced(tenantNamespace, schemaValidationEnforced)
    namespaces().setSchemaCompatibilityStrategy(tenantNamespace, schemaCompatibilityStrategy)
}

suspend fun PulsarAdmin.createTenantAndNamespace(tenantId: String, namespace: String, allowTopicCreation: Boolean = false, isAllowAutoUpdateSchema: Boolean = false, schemaValidationEnforced: Boolean = true, schemaCompatibilityStrategy: SchemaCompatibilityStrategy = SchemaCompatibilityStrategy.FULL_TRANSITIVE) = withContext(Dispatchers.VirtualThreads) {

    createTenant(tenantId)
    createNamespace(tenantId, namespace)
    configureNamespace(tenantId, namespace, allowTopicCreation, isAllowAutoUpdateSchema, schemaValidationEnforced, schemaCompatibilityStrategy)
}

suspend fun PulsarAdmin.ensureTenantAndNamespaceExist(tenantId: String, namespace: String, allowTopicCreation: Boolean = false, isAllowAutoUpdateSchema: Boolean = false, schemaValidationEnforced: Boolean = true, schemaCompatibilityStrategy: SchemaCompatibilityStrategy = SchemaCompatibilityStrategy.FULL_TRANSITIVE) {

    ensureTenantExists(tenantId)
    val newNamespaceWasCreated = ensureNamespaceExists(tenantId, namespace)
    if (newNamespaceWasCreated) {
        configureNamespace(tenantId, namespace, allowTopicCreation, isAllowAutoUpdateSchema, schemaValidationEnforced, schemaCompatibilityStrategy)
    }
}

suspend fun PulsarAdmin.ensureTenantExists(tenantId: String): Boolean = withConflictExceptionIgnored {
    createTenant(tenantId)
}

suspend fun PulsarAdmin.ensureNamespaceExists(tenantId: String, namespace: String): Boolean = withConflictExceptionIgnored {
    createNamespace(tenantId, namespace)
}

suspend fun PulsarAdmin.ensureTopicExists(fullyQualifiedTopic: String, numberOfPartitions: Int = 1): Boolean = withConflictExceptionIgnored {
    createTopic(fullyQualifiedTopic, numberOfPartitions)
}

suspend fun PulsarAdmin.ensureTopicWorks(topic: PulsarTopic, numberOfPartitions: Int = 1, allowTopicCreation: Boolean = false, isAllowAutoUpdateSchema: Boolean = false, schemaValidationEnforced: Boolean = true, schemaCompatibilityStrategy: SchemaCompatibilityStrategy = SchemaCompatibilityStrategy.FULL_TRANSITIVE, schema: Schema<*>? = null): Boolean = withContext(Dispatchers.VirtualThreads) {

    ensureTenantAndNamespaceExist(topic.tenant, topic.namespace, allowTopicCreation, isAllowAutoUpdateSchema, schemaValidationEnforced, schemaCompatibilityStrategy)
    val existing = ensureTopicExists(topic.fullName, numberOfPartitions)
    schema?.schemaInfo?.let { schemas().createSchema(topic.fullName, it) }
    existing
}

private inline fun withConflictExceptionIgnored(action: () -> Unit): Boolean = try {
    action()
    true
} catch (error: PulsarAdminException.ConflictException) {
    false
}