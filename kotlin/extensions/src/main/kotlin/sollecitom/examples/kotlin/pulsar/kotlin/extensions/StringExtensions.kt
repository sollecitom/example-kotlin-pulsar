package sollecitom.examples.kotlin.pulsar.kotlin.extensions

fun String.replaceFrom(delimiter: String, replacement: String, missingDelimiterValue: String = this): String {

    val index = indexOf(delimiter)
    return if (index == -1) missingDelimiterValue else replaceRange(index, length, replacement)
}