package sollecitom.examples.kotlin.pulsar.test.utils

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.admin.PulsarAdminBuilder
import org.apache.pulsar.client.api.ClientBuilder
import org.apache.pulsar.client.api.PulsarClient
import org.testcontainers.containers.Network
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.toJavaDuration

private const val DEFAULT_PULSAR_DOCKER_IMAGE_VERSION = "3.0.0"
private const val PULSAR_NETWORK_ALIAS = "pulsar"
private const val PULSAR_IMAGE_NAME = "apachepulsar/pulsar"

private fun pulsarDockerImageName(version: String) = DockerImageName.parse("$PULSAR_IMAGE_NAME:$version")

fun newPulsarContainer(version: String = DEFAULT_PULSAR_DOCKER_IMAGE_VERSION, startupAttempts: Int = 10, startupTimeout: Duration = 2.minutes): PulsarContainer {

    return PulsarContainer(pulsarDockerImageName(version))
            .withStartupAttempts(startupAttempts)
            .withStartupTimeout(startupTimeout.toJavaDuration())
            .apply { setWaitStrategy(PulsarWaitStrategies.clustersEndpoint) }
}

fun PulsarContainer.withNetworkAndAliases(network: Network, vararg aliases: String = arrayOf(PULSAR_NETWORK_ALIAS)): PulsarContainer = withNetwork(network).withNetworkAliases(*aliases)

val PulsarContainer.networkAlias: String get() = PULSAR_NETWORK_ALIAS

fun PulsarContainer.client(customise: ClientBuilder.() -> Unit = {}): PulsarClient = PulsarClient.builder().serviceUrl(pulsarBrokerUrl).also(customise).build()

fun PulsarContainer.admin(customise: PulsarAdminBuilder.() -> Unit = {}): PulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(httpServiceUrl).also(customise).build()

