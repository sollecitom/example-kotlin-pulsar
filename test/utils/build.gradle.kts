dependencies {
    api(projects.pulsarKotlinExampleKotlinExtensions)
    api(platform(libs.kotlinx.coroutines.bom))
    api(libs.kotlinx.coroutines.test)

    api(libs.junit.jupiter.api)
    api(platform(libs.strikt.bom))
    api(libs.strikt.core)
    api(libs.strikt.jvm)
    api(libs.strikt.mockk)

    runtimeOnly(libs.junit.jupiter.engine)
}