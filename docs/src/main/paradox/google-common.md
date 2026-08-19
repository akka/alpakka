# Google Common

The `google-common` module provides central configuration for Google connectors in Alpakka as well as basic support for interfacing with Google APIs.

## Artifacts

@@@note
The Akka dependencies are available from Akka’s secure library repository. To access them you need to use a secure, tokenized URL as specified at https://account.akka.io/token.
@@@

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

The `alpakka.google.credentials.provider` setting selects where credentials come from:

| `provider`                      | Credentials |
|---------------------------------|-------------|
| `application-default` (default) | Tries `service-account`, then `compute-engine` |
| `service-account`               | A service account key, from your configuration file or a credentials file |
| `compute-engine`                | The service account of the [Compute Engine](https://cloud.google.com/compute) instance, read from the metadata server |
| `user-access`                   | A user refresh token, from your configuration file or a credentials file |
| `none`                          | No credentials; requests are sent unauthenticated |

With the default `application-default` provider, credentials will be loaded automatically:

1. From the file path specified by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or another [“well-known” location](https://medium.com/google-cloud/use-google-cloud-user-credentials-when-testing-containers-locally-acb57cd4e4da); or
2. When running in a [Compute Engine](https://cloud.google.com/compute) instance, from the metadata server at `metadata.google.internal`.

Credentials can also be specified manually in your configuration file.

@@@ warning

If neither source yields credentials, `application-default` does not fail: it logs a warning and falls back to
the `none` provider. Requests are then sent without valid credentials and are rejected by Google APIs, typically
with `401 Unauthorized` or `403 Forbidden`, which looks like a permissions problem rather than a configuration
problem. Set `provider` explicitly if this fallback is not what you want.

Reading credentials from the Compute Engine metadata server blocks during settings initialization for at most
`alpakka.google.credentials.compute-engine.timeout`. A timeout means the metadata server
could not be reached — because the application is not running on Google Cloud, or because the request was
intercepted or blocked by a proxy or service mesh — and leads to the same fallback. The warning names both the
service account and the Compute Engine failure, so check it before investigating IAM.

@@@

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
