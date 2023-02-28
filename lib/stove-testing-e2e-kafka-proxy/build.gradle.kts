dependencies {
    api(project(":lib:stove-testing-e2e"))
    implementation(libs.netty.all)
    implementation(libs.kafka)
    implementation(libs.apache.avro)
    implementation(libs.kotlinx.io.reactor.extensions)
    implementation(libs.kotlinx.reactive)
    implementation(libs.kotlinx.jdk8)
    implementation(libs.kotlinx.core)
    implementation(libs.slf4j.api)
}

dependencies {
    testImplementation(libs.slf4j.simple)
}
