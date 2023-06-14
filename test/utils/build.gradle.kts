dependencies {
    api(libs.junit.jupiter.api)
    api(platform(libs.strikt.bom))
    api(libs.strikt.core)
    api(libs.strikt.jvm)
    api(libs.strikt.mockk)

    runtimeOnly(libs.junit.jupiter.engine)
}