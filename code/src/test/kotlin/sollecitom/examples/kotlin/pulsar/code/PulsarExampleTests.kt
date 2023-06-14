package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import reactor.blockhound.BlockHound
import reactor.blockhound.BlockingOperationError
import sollecitom.examples.kotlin.pulsar.test.utils.admin
import sollecitom.examples.kotlin.pulsar.test.utils.client
import sollecitom.examples.kotlin.pulsar.test.utils.newPulsarContainer
import strikt.api.expect
import strikt.api.expectThrows
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
        BlockHound.builder().loadIntegrations().install()
        DebugProbes.install()
        pulsar.start()
    }

    @AfterAll
    fun afterAll() {
        pulsarAdmin.close()
        pulsarClient.close()
        pulsar.stop()
        DebugProbes.uninstall()
    }

    @Test
    fun `something with Pulsar`() = runTest(timeout = timeout) {

        expect {
            that(true) {
                isTrue()
            }
        }
    }

    @Test
    @Suppress("BlockingMethodInNonBlockingContext")
    fun `blockhound works correctly`() = runTest(timeout = timeout) {

        expectThrows<BlockingOperationError> {
            withContext(Dispatchers.Default) {
                Thread.sleep(1)
            }
        }
    }
}