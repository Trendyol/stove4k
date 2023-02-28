dependencies {
    implementation(libs.kotlinx.io.reactor.extensions)
    implementation(libs.kotlinx.reactive)
    implementation(libs.kotlinx.jdk8)
    implementation(libs.kotlinx.core)
    implementation(libs.kafka)
    implementation(libs.slf4j.api)
}

dependencies {
    testImplementation(libs.slf4j.simple)
}
