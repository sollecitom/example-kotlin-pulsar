package sollecitom.examples.kotlin.pulsar.kotlin.extensions

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transformWhile

fun <T> Flow<T>.takeUntil(predicate: (T) -> Boolean) = transformWhile {
    emit(it)
    predicate(it)
}