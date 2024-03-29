# What is Stove?

Stove is an end-to-end testing framework that spins up physical dependencies and your application all together. So you
have a control over dependencies via Kotlin code. 
Your tests will be infra agnostic, but component aware, so they can use easily necessary physical components with Stove provided APIs. 
All the infra is **pluggable**, and can be added easily. You can also create your own infra needs by using the abstractions
that Stove provides.
Having said that, the only dependency is `docker` since Stove is
using [testcontainers](https://github.com/testcontainers/testcontainers-java) underlying.

You can use JUnit and Kotest for running the tests. You can run all the tests on your CI, too.
But that needs **DinD(docker-in-docker)** integration.

The medium story about the motivation behind the framework:
[A New Approach to the API End-to-End Testing in Kotlin](https://medium.com/trendyol-tech/a-new-approach-to-the-api-end-to-end-testing-in-kotlin-f743fd1901f5)

## High Level Architecture

![img](./assets/stove_architecture.svg)

## How to build the source code?

- JDK 16+
- Docker for running the tests (please use the latest version)

```shell
./gradlew build # that will build and run the tests
```

## How to get?

The framework still under development and is getting matured. In general it is working well and in use at Trendyol.
Since it should be located under your testing context it is risk-free to apply and use, give it a try!

The current version is going with strategy of `SNAPSHOT` hence you can get the library from the snapshot repository.

Overview of the [snapshot versions of Stove](https://oss.sonatype.org/#nexus-search;gav~com.trendyol~stove-*~~~)

`$version = please check the current version`

=== "Gradle"

    You need to enable snapshot repository settings first to get it. Navigate to the file that you define your repositories.
    ```kotlin hl_lines="3-5"
    repositories {
        mavenCentral() // you probably have already
        maven { // code to be added
            url = uri("https://oss.sonatype.org/content/repositories/snapshots")
        }
    }
    ```
    Now, navigate to your dependencies section, and add the dependencies according to your testing needs.

    ``` kotlin
    dependencies {
        testImplementation("com.trendyol:stove-testing-e2e:$version")
    }
    ```

=== "Maven"
    
    You need to enable snapshot repository settings in `~/.m2/settings.xml` or settings.xml file in your project folder.
    Modifying `pom.xml` also works. Either way, make sure that these xml block is located in one of the places.

    If you can't manage please look at this [StackOverflow answer.](https://stackoverflow.com/questions/7715321/how-to-download-snapshot-version-from-maven-snapshot-repository)

    ``` xml
      <profiles>
      <profile>
         <id>allow-snapshots</id>
            <activation>
                <activeByDefault>true</activeByDefault>
            </activation>
         <repositories>
           <repository>
             <id>snapshots-repo</id>
             <url>https://oss.sonatype.org/content/repositories/snapshots</url>
             <releases><enabled>false</enabled></releases>
             <snapshots><enabled>true</enabled></snapshots>
           </repository>
         </repositories>
       </profile>
    </profiles>
    ```
    
    Now you can add dependencies.

    ```xml
     <dependency>
        <groupId>com.trendyol</groupId>
        <artifactId>stove-testing-e2e</artifactId>
        <version>${stove-version}</version>
     </dependency>

      <dependency>
        <groupId>com.trendyol</groupId>
        <artifactId>stove-testing-e2e-couchbase</artifactId>
        <version>${stove-version}</version>
     </dependency> 

      <dependency>
        <groupId>com.trendyol</groupId>
        <artifactId>stove-testing-e2e-http</artifactId>
        <version>${stove-version}</version>
     </dependency> 

      <dependency>
        <groupId>com.trendyol</groupId>
        <artifactId>stove-testing-e2e-wiremock</artifactId>
        <version>${stove-version}</version>
     </dependency> 
    ```

### [How To Write Tests?](./how-to-write-tests)

You can start looking at the ways of testing an application with Stove. These are explained in detail under the
corresponding sections.

#### [1. Application Aware _(recommended)_](./how-to-write-tests/1.Application-Aware)

#### [2. Dockerized](./how-to-write-tests/2.Dockerized)
