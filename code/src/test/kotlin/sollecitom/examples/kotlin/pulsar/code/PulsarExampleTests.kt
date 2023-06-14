package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import sollecitom.examples.kotlin.pulsar.test.utils.admin
import sollecitom.examples.kotlin.pulsar.test.utils.client
import sollecitom.examples.kotlin.pulsar.test.utils.newPulsarContainer
import strikt.api.expectThat
import strikt.assertions.isTrue
import kotlin.time.Duration.Companion.seconds

@TestInstance(PER_CLASS)
private class PulsarExampleTests {

    private val timeout = 10.seconds
    private val pulsar = newPulsarContainer()
    private val pulsarClient by lazy(pulsar::client)
    private val pulsarAdmin by lazy(pulsar::admin)

    @BeforeAll
    fun beforeAll() {
        DebugProbes.install()
        pulsar.start()
    }

    @AfterAll
    fun afterAll() {
        DebugProbes.uninstall()
        pulsarAdmin.close()
        pulsarClient.close()
        pulsar.stop()
    }

    @Test
    fun `something with Pulsar`() = runTest(timeout = timeout) {

        expectThat(true) {
            isTrue()
        }
    }
}