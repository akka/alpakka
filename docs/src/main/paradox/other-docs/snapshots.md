# Snapshots 

[bintray-badge]:  https://api.bintray.com/packages/akka/snapshots/alpakka/images/download.svg
[bintray]:        https://bintray.com/akka/snapshots/alpakka/_latestVersion 

Snapshots are published daily to a repository in bintray. Add the following to your project build definition to resolve Alpakka snapshots:

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


Last published snapshot version is [![bintray-badge][]][bintray]

