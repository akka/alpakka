# Google FCM

@@@ note { title="Google Firebase Cloud Messaging" }

Google Firebase Cloud Messaging (FCM) is a cross-platform messaging solution that lets you reliably deliver messages at no cost.

Using FCM, you can notify a client app that new email or other data is available to sync. You can send notification messages to drive user re-engagement and retention. For use cases such as instant messaging, a message can transfer a payload of up to 4KB to a client app.

@@@

The Alpakka Google Firebase Cloud Messaging connector provides a way to send notifications with [Firebase Cloud Messaging](https://firebase.google.com/docs/cloud-messaging/).

@@project-info{ projectId="google-fcm" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-fcm_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
  symbol3=AkkaHttpVersion
  value3=$akka-http.version$
  group3=com.typesafe.akka
  artifact3=akka-http_$scala.binary.version$
  version3=AkkaHttpVersion
  group4=com.typesafe.akka
  artifact4=akka-http-spray-json_$scala.binary.version$
  version4=AkkaHttpVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="google-fcm" }


## Settings

Shared settings for all Google connectors are read by default from the `alpakka.google` configuration section in your `application.conf`.
Credentials will be loaded automatically:

1. From the file path specified by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable or another [“well-known” location](https://medium.com/google-cloud/use-google-cloud-user-credentials-when-testing-containers-locally-acb57cd4e4da); or
2. When running in a [Compute Engine](https://cloud.google.com/compute) instance.

Credentials can also be specified manually in your configuration file.

All of the common configuration settings for Google connectors can be found in the @github[reference.conf](/google-cloud-common/src/main/resources/reference.conf).
Additional FCM-specific configuration settings can be found in its own @github[reference.conf](/google-fcm/src/main/resources/reference.conf).

@@snip [snip](/google-fcm/src/test/resources/application.conf) { #init-credentials }

The last two parameters in the above example are the predefined values.
You can send test notifications [(so called validate only).](https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages/send)
And you can set the number of maximum concurrent connections.
There is a limitation in the docs; from one IP you can have maximum 1k pending connections,
and you may need to configure `akka.http.host-connection-pool.max-open-requests` in your application.conf.


## Sending notifications

To send a notification message create your notification object, and send it!

Scala
: @@snip [snip](/google-fcm/src/test/scala/docs/scaladsl/FcmExamples.scala) { #imports #asFlow-send }

Java
: @@snip [snip](/google-fcm/src/test/java/docs/javadsl/FcmExamples.java) { #imports #asFlow-send }

With this type of send you can get responses from the server.
These responses can be @scaladoc[FcmSuccessResponse](akka.stream.alpakka.google.firebase.fcm.FcmSuccessResponse) or @scaladoc[FcmErrorResponse](akka.stream.alpakka.google.firebase.fcm.FcmErrorResponse).
You can choose what you want to do with this information, but keep in mind
if you try to resend the failed messages you will need to use exponential backoff! (see [Akka docs `RestartFlow.onFailuresWithBackoff`](https://doc.akka.io/docs/akka/current/stream/operators/RestartFlow/onFailuresWithBackoff.html))

If you don't care if the notification was sent successfully, you may use `fireAndForget`.

Scala
: @@snip [snip](/google-fcm/src/test/scala/docs/scaladsl/FcmExamples.scala) { #imports #simple-send }

Java
: @@snip [snip](/google-fcm/src/test/java/docs/javadsl/FcmExamples.java) { #imports #simple-send }

With fire and forget you will just send messages and ignore all the errors.

To help the integration and error handling or logging, there is a variation of the flow where you can send data beside your notification.

Scala
: @@snip [snip](/google-fcm/src/test/scala/docs/scaladsl/FcmExamples.scala) { #imports #withData-send }

Java
: @@snip [snip](/google-fcm/src/test/java/docs/javadsl/FcmExamples.java) { #imports #withData-send }

Here I send a simple string, but you could use any type.

## Scala only

You can build any notification described in the original documentation.
It can be done by hand, or using some builder method.
If you build your notification from scratch with options (and not with the provided builders), worth to check isSendable before sending.

Scala
: @@snip [snip](/google-fcm/src/test/scala/docs/scaladsl/FcmExamples.scala) { #noti-create }

There is a condition builder too.

Scala
: @@snip [snip](/google-fcm/src/test/scala/docs/scaladsl/FcmExamples.scala) { #condition-builder }
