dependencies {
    api(projects.lib.stoveTestingE2e)
    implementation(libs.mongodb.reactivestreams)
    implementation(libs.kotlinx.io.reactor.extensions)
    implementation(libs.kotlinx.reactive)
    implementation(libs.kotlinx.jdk8)
    implementation(libs.kotlinx.core)
    implementation(libs.testcontainers.mongodb)
}

dependencies {
    testImplementation(libs.slf4j.simple)
}
