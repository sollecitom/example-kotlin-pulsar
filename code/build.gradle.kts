dependencies {
    implementation(platform(libs.kotlinx.coroutines.bom))


    testImplementation(libs.junit.jupiter.api)
    testImplementation(platform(libs.strikt.bom))
    testImplementation(libs.strikt.core)
    testImplementation(libs.strikt.jvm)
    testImplementation(libs.strikt.mockk)

    testRuntimeOnly(libs.junit.jupiter.engine)
}