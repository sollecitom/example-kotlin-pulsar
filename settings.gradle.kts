@file:Suppress("UnstableApiUsage")

rootProject.name = "pulsar-kotlin-example"
val versionCatalogName: String by settings

module("code")
module("test", "utils")
module("kotlin", "extensions")
module("messaging", "domain")
module("pulsar", "domain")
module("pulsar", "test", "utils")

module("z_craftsmanship", "example")
module("z_craftsmanship", "test", "utils")

fun module(vararg pathSegments: String) {
    val projectName = pathSegments.last()
    val path = pathSegments.dropLast(1)
    val group = path.joinToString(separator = "-")
    val directory = path.joinToString(separator = "/", prefix = "./")

    include("${rootProject.name}-${if (group.isEmpty()) "" else "$group-"}$projectName")
    project(":${rootProject.name}-${if (group.isEmpty()) "" else "$group-"}$projectName").projectDir = mkdir("$directory/$projectName")
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")