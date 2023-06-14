package sollecitom.examples.kotlin.pulsar.code

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import strikt.api.expectThat
import strikt.assertions.isTrue

@TestInstance(PER_CLASS)
private class PulsarExampleTests {

    @Test
    fun `something with Pulsar`() {

        expectThat(true) {
            isTrue()
        }
    }
}