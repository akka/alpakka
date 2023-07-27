# Google Common

The `google-common` module provides central configuration for Google connectors in Alpakka as well as basic support for interfacing with Google APIs.

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [Maven,sbt,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-stream-alpakka-google-common_$scala.binary.version$
version=$project.version$
symbol2=AkkaVersion
value2=$akka.version$
group2=com.typesafe.akka
artifact2=akka-stream_$scala.binary.version$
version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="google-common" }

## Configuration

Shared settings for all Google connectors are read by default from the `alpakka.google` configuration section in your `application.conf`.
The available options and their default values are documented in the `reference.conf`.
If you use a non-standard configuration path or need multiple different configurations please refer to the sections below.

@@snip [reference.conf](/google-common/src/main/resources/reference.conf)

## Credentials

Credentials will be loaded automatically:

1. From the file path specified by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or another [“well-known” location](https://medium.com/google-cloud/use-google-cloud-user-credentials-when-testing-containers-locally-acb57cd4e4da); or
2. When running in a [Compute Engine](https://cloud.google.com/compute) instance.

Credentials can also be specified manually in your configuration file.

## Accessing settings

@apidoc[GoogleSettings$] provides methods to retrieve settings from your configuration and @apidoc[GoogleAttributes$] to access the settings attached to a stream.
@scala[Additionally, if there is an implicit @apidoc[akka.actor.ActorSystem] in scope, then so will be an implicit instance of the default @apidoc[GoogleSettings].]

Scala

: @@snip [snip](/google-common/src/test/scala/docs/scaladsl/GoogleCommonDoc.scala) { #accessing-settings }

Java

: @@snip [snip](/google-common/src/test/java/docs/javadsl/GoogleCommonDoc.java) { #accessing-settings }

## Apply custom settings to a part of the stream

In certain situations it may be desirable to modify the @apidoc[GoogleSettings] applied to a part of the stream, for example to use different credentials or change the @apidoc[akka.stream.alpakka.google.RetrySettings].
This is accomplished by adding @apidoc[GoogleAttributes$] to your stream.

Scala

: @@snip [snip](/google-common/src/test/scala/docs/scaladsl/GoogleCommonDoc.scala) { #custom-settings }

Java

: @@snip [snip](/google-common/src/test/java/docs/javadsl/GoogleCommonDoc.java) { #custom-settings }

## Interop with Google Java client libraries

Instances of the @apidoc[akka.stream.alpakka.google.auth.Credentials] class can be converted via the `toGoogle()` method to @javadoc[Credentials](com.google.auth.Credentials) compatible with Google Java client libraries.

## Accessing other Google APIs

The @apidoc[Google$] @scala[object] @java[class] provides methods for interfacing with Google APIs.
You can use it to access APIs that are not currently supported by Alpakka and build new connectors.
