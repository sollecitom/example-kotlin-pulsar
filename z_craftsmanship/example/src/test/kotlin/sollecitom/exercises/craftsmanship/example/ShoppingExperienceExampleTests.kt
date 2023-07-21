package sollecitom.exercises.craftsmanship.example

import assertk.assertThat
import assertk.assertions.containsExactlyInAnyOrder
import assertk.assertions.isEqualTo
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import java.time.Instant
import java.util.*

@TestInstance(PER_CLASS)
private class ShoppingExperienceExampleTests {

    @Test
    fun `a user adds items to their cart`() {

        val user = newUser()
        val banana = newProduct("banana")
        val apple = newProduct("apple")

        user.addToCart(2 * banana)
        user.addToCart(2 * apple)
        user.addToCart(banana)

        assertThat(user.cart).containsExactlyInAnyOrder(3 * banana, 2 * apple)
    }

    @Test
    fun `a user pays for the products they put in their cart`() {

        val user = newUser()
        val productQuantity = 2
        val productPrice = 5.0
        val product = newProduct()
        val shop = newShop(prices = mapOf(product to productPrice))

        user.addToCart(productQuantity * product)

        val bill = user.checkout(shop)

        assertThat(bill::totalPrice).isEqualTo(productQuantity * productPrice)
    }

    @Test
    fun `a user pays nothing for an empty cart`() {

        val user = newUser()
        val shop = newShop(prices = emptyMap())

        val bill = user.checkout(shop)

        assertThat(bill::totalPrice).isEqualTo(0.0)
    }

    private fun newProduct(id: String = UUID.randomUUID().toString()): Product = InMemoryProduct(id)

    private fun newUser(id: String = UUID.randomUUID().toString()): User = InMemoryUser(id)

    private fun newShop(prices: Map<Product, Double>, id: String = UUID.randomUUID().toString()): Shop = InMemoryShop(id, InMemoryPricesRegistry(prices))
}

internal class InMemoryPricesRegistry(private val prices: Map<Product, Double>) : ProductPricesRegistry {

    override fun forProduct(product: Product, time: Instant): Double {
        return prices[product]!!
    }
}

data class InMemoryShop(val id: String, private val prices: ProductPricesRegistry) : Shop {

    override fun billFor(cart: Cart, time: Instant): Bill {

        var totalPrice = 0.0
        cart.forEach { item ->
            totalPrice += item.count.value * prices.forProduct(item.product, time)
        }
        return Bill(totalPrice = totalPrice)
    }
}

interface Shop {
    fun billFor(cart: Cart, time: Instant): Bill
}

internal data class InMemoryUser(private val id: String) : User {

    private val _cart = InMemoryMutableCart()
    override val cart: Cart get() = _cart

    override fun addToCart(item: ProductQuantity) = _cart.add(item)

    override fun checkout(shop: Shop): Bill = shop.billFor(cart, Instant.now())
}

interface ProductPricesRegistry {

    fun forProduct(product: Product, time: Instant): Double
}

internal class InMemoryMutableCart : Cart {

    private val countByProduct = mutableMapOf<Product, Count>()

    override val size: Int get() = countByProduct.size

    fun add(item: ProductQuantity) {

        countByProduct.compute(item.product) { _, existingCount -> Count((existingCount?.value ?: 0) + item.count.value) }
    }

    override fun contains(element: ProductQuantity) = countByProduct[element.product]?.let { it == element.count } ?: false

    override fun containsAll(elements: Collection<ProductQuantity>) = elements.all { contains(it) }

    override fun isEmpty() = countByProduct.isEmpty()

    override fun iterator() = countByProduct.entries.map { ProductQuantity(it.key, it.value) }.iterator()
}

internal data class InMemoryProduct(override val id: String) : Product

operator fun Int.times(product: Product) = ProductQuantity(product, Count(this))

data class ProductQuantity(val product: Product, val count: Count)

data class Count(val value: Int) {

    init {
        require(value >= 0) { "Value cannot be negative" }
    }
}

interface Product {

    val id: String
}

interface User {

    val cart: Cart

    fun addToCart(item: ProductQuantity)

    fun checkout(shop: Shop): Bill
}

data class Bill(val totalPrice: Double)

fun User.addToCart(product: Product) = addToCart(1 * product)

interface Cart : Collection<ProductQuantity>
