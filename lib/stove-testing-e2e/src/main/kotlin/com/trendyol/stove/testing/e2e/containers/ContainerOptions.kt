package com.trendyol.stove.testing.e2e.containers

/**
 * Container options to run
 */
interface ContainerOptions {
    val registry: String

    val image: String

    val tag: String

    val imageWithTag: String get() = "$image:$tag"

    val compatibleSubstitute: String?
}
