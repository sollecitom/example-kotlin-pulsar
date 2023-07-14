package sollecitom.exercises.craftsmanship.example

import assertk.assertThat
import assertk.assertions.containsOnly
import assertk.assertions.isFalse
import assertk.assertions.isTrue
import kotlinx.datetime.*
import kotlinx.datetime.DateTimeUnit.Companion.DAY
import kotlinx.datetime.TimeZone
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import java.util.*
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

@TestInstance(PER_CLASS)
private class ScratchpadTest {

    @Test
    fun `scratchpad test`() {

        val immortalYogurt = ImmortalFood()
        val mortalButter = SimplePerishableFood(expiryDate = today() - 2.days)
        val mortalYogurtThatWasOpened = ComplicatedPerishableFood(expiryDate = today() + 10.days, openedAt = today() - 6.days, shelfLifeAfterBeingOpen = 5.days)

        val expired = findExpired(listOf(immortalYogurt, mortalButter, mortalYogurtThatWasOpened))

        assertThat(expired).containsOnly(mortalButter, mortalYogurtThatWasOpened)
    }

    @Test
    fun `mortal butter did expire`() {

        val mortalButter = SimplePerishableFood(expiryDate = today() - 2.days)

        val didExpire = mortalButter.isExpired()

        assertThat(didExpire).isTrue()
    }

    @Test
    fun `mortal butter did not expire`() {

        val expiredAt = today()
        val mortalButter = SimplePerishableFood(expiryDate = expiredAt)
        val timeNow = expiredAt - 1.days

        val didExpire = mortalButter.isExpiredAtTime(timeNow)

        assertThat(didExpire).isFalse()
    }
}

fun findExpired(foods: List<CanExpire>, timeNow: LocalDate = today()): List<CanExpire> {

    return foods.filter { it.isExpiredAtTime(timeNow) }
}

interface CanExpire {

    fun isExpiredAtTime(time: LocalDate): Boolean

    fun isExpired() = isExpiredAtTime(today())
}

class ImmortalFood : CanExpire {

    override fun isExpiredAtTime(time: LocalDate) = false
}

class SimplePerishableFood(private val expiryDate: LocalDate) : CanExpire {

    override fun isExpiredAtTime(time: LocalDate) = expiryDate < time
}

class ComplicatedPerishableFood(private val expiryDate: LocalDate, private val openedAt: LocalDate, private val shelfLifeAfterBeingOpen: Duration) : CanExpire {

    override fun isExpiredAtTime(time: LocalDate): Boolean {
        if (openedAt < time - shelfLifeAfterBeingOpen) {
            return true
        }
        return expiryDate < time
    }
}

private fun today(timeZone: TimeZone = TimeZone.currentSystemDefault()) = Clock.System.todayIn(timeZone)

private operator fun LocalDate.plus(duration: Duration): LocalDate = plus(duration.inWholeDays, DAY)
private operator fun LocalDate.minus(duration: Duration): LocalDate = minus(duration.inWholeDays, DAY)