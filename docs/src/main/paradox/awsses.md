# AWS SES Connector

The AWS SES Connector provides Akka Stream Flow for AWS SES integration.

For more information about Amazon Simple Email Service (Amazon SES) please visit the [official documentation](https://aws.amazon.com/documentation/ses/).

## Artifacts

sbt
:   @@@vars
    ```scala
    libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-awsses" % "$version$"
    ```
    @@@

Maven
:   @@@vars
    ```xml
    <dependency>
      <groupId>com.lightbend.akka</groupId>
      <artifactId>akka-stream-alpakka-awsses_$scalaBinaryVersion$</artifactId>
      <version>$version$</version>
    </dependency>
    ```
    @@@

Gradle
:   @@@vars
    ```gradle
    dependencies {
      compile group: "com.lightbend.akka", name: "akka-stream-alpakka-awsses_$scalaBinaryVersion$", version: "$version$"
    }
    ```
    @@@
