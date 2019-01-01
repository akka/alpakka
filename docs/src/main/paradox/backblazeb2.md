# AWS Lambda

The Backblaze B2 connector provides Akka Flows for Backblaze B2 integration.

For more information about Backblaze please visit the [Backblaze B2 documentation](https://www.backblaze.com/b2/docs/).

@@project-info{ projectId="backblazeb2" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-backblazeb2_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="backblazeb2" }

## Usage

Flows provided by this connector are provided by `B2Streams` which takes a `B2AccountCredentials` parameter
as well as @scaladoc[ActorSystem](akka.actor.ActorSystem) and @scaladoc[ActorMaterializer](akka.stream.ActorMaterializer)
implicit parameters.
