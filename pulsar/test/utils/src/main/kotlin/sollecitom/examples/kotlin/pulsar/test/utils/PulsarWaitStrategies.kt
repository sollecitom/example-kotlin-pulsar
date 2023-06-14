package sollecitom.examples.kotlin.pulsar.test.utils

import com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy
import org.testcontainers.containers.wait.strategy.Wait

internal object PulsarWaitStrategies {

    private const val BROKER_HTTP_PORT = 8080
    private const val ADMIN_CLUSTERS_ENDPOINT = "/admin/v2/clusters"

    val clustersEndpoint: HttpWaitStrategy = Wait.forHttp(ADMIN_CLUSTERS_ENDPOINT).forPort(BROKER_HTTP_PORT).forStatusCode(HttpStatus.SC_OK).forResponsePredicate("[\"standalone\"]"::equals)
}