dependencies {
    implementation(platform(libs.kotlinx.coroutines.bom))

    testImplementation(projects.pulsarKotlinExampleTestUtils)
}