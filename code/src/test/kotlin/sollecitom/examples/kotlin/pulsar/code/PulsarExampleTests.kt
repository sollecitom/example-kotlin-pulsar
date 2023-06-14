package sollecitom.examples.kotlin.pulsar.code

import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import strikt.api.expectThat
import strikt.assertions.isFalse
import strikt.assertions.isTrue
import kotlin.time.Duration.Companion.seconds

@TestInstance(PER_CLASS)
private class PulsarExampleTests {

    private val timeout = 10.seconds

    @Test
    fun `something with Pulsar`() = runTest(timeout = timeout) {

        expectThat(true) {
            isTrue()
            isFalse()
        }
    }
}