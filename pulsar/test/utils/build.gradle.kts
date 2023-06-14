dependencies {
    api(projects.pulsarKotlinExamplePulsarDomain)
    api(libs.testcontainers.pulsar)

    implementation(projects.pulsarKotlinExampleKotlinExtensions)
}