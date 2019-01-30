# Snapshots 

[bintray-badge]:  https://api.bintray.com/packages/akka/snapshots/alpakka/images/download.svg
[bintray]:        https://bintray.com/akka/snapshots/alpakka/_latestVersion 

Snapshots are published to a repository in Bintray after every successful build on master. Add the following to your project build definition to resolve Alpakka snapshots:

## Configure repository

sbt
:   ```scala
    resolvers += Resolver.bintrayRepo("akka", "snapshots")
    ```

Maven
:   ```xml
    <project>
    ...
      <repositories>
        <repository>
          <id>alpakka-snapshots</id>
          <name>Alpakka Snapshots</name>
          <url>https://dl.bintray.com/akka/snapshots</url>
        </repository>
      </repositories>
    ...
    </project>
    ```

Gradle
:   ```gradle
    repositories {
      maven {
        url  "https://dl.bintray.com/akka/snapshots"
      }
    }
    ```

## Documentation

The [snapshot documentation](https://doc.akka.io/docs/alpakka/snapshot/) is updated with every snapshot build.


## Versions

Latest published snapshot version is [![bintray-badge][]][bintray]

The snapshot repository is cleaned from time to time with no further notice. Check [Bintray Alpakka files](https://bintray.com/akka/snapshots/alpakka#files/com/lightbend/akka) to see what versions are currently available.
