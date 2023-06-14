dependencies {
    api(projects.pulsarKotlinExampleMessagingDomain)

    api(libs.pulsar.admin.client) {
        // non-bc-fips deps bundle
        exclude(group = "org.apache.pulsar", module = "bouncy-castle-bc")
    }
    // api(libs.pulsar.crypto.bcfips) { artifact { classifier = "pkg" } } // for some reason if we use this line instead of the one below, it does not work in downstream projects
    api("org.apache.pulsar:bouncy-castle-bcfips:${libs.pulsar.client.get().version.toString()}:pkg")

    implementation(projects.pulsarKotlinExampleKotlinExtensions)
}