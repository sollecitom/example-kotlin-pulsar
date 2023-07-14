package sollecitom.exercises.craftsmanship.example

import assertk.assertThat
import assertk.assertions.isFalse
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS

@TestInstance(PER_CLASS)
private class ScratchpadTest {

    @Test
    fun `scratchpad test`() {

        assertThat(true).isFalse()
    }
}