# Huawei Push Kit

@@@ note { title="Huawei Push Kit" }

Huawei Push Kit is a messaging service provided for you. It establishes a messaging channel from the cloud to devices. By integrating Push Kit, you can send messages to your apps on users' devices in real time.

@@@

The Alpakka Huawei Push Kit connector provides a way to send notifications with [Huawei Push Kit](https://developer.huawei.com/consumer/en/hms/huawei-pushkit).

@@project-info{ projectId="huawei-push-kit" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-stream-alpakka-huawei-push-kit_$scala.binary.version$
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

@@dependencies { projectId="huawei-push-kit" }

## Settings

All of the configuration settings for Huawei Push Kit can be found in the @github[reference.conf](/huawei-push-kit/src/main/resources/reference.conf).

@@snip [snip](/huawei-push-kit/src/test/resources/application.conf) { #init-credentials }

The `test` and `maxConcurrentConnections`  parameters in @scaladoc[HmsSettings](akka.stream.alpakka.huawei.pushkit.HmsSettings) are the predefined values.
You can send test notifications [(so called validate only).](https://developer.huawei.com/consumer/en/doc/development/HMSCore-References-V5/https-send-api-0000001050986197-V5)
And you can set the number of maximum concurrent connections.

## Sending notifications

To send a notification message create your notification object, and send it!

Scala
: @@snip [snip](/huawei-push-kit/src/test/scala/docs/scaladsl/PushKitExamples.scala) { #imports #asFlow-send }

Java
: @@snip [snip](/huawei-push-kit/src/test/java/docs/javadsl/PushKitExamples.java) { #imports #asFlow-send }

With this type of send you can get responses from the server.
These responses can be @scaladoc[PushKitResponse](akka.stream.alpakka.huawei.pushkit.PushKitResponse) or @scaladoc[ErrorResponse](akka.stream.alpakka.huawei.pushkit.ErrorResponse).
You can choose what you want to do with this information, but keep in mind
if you try to resend the failed messages you will need to use exponential backoff! (see @extref[[Akka docs `RestartFlow.onFailuresWithBackoff`](akka:stream/operators/RestartFlow/onFailuresWithBackoff.html))

If you don't care if the notification was sent successfully, you may use `fireAndForget`.

Scala
: @@snip [snip](/huawei-push-kit/src/test/scala/docs/scaladsl/PushKitExamples.scala) { #imports #simple-send }

Java
: @@snip [snip](/huawei-push-kit/src/test/java/docs/javadsl/PushKitExamples.java) { #imports #simple-send }

With fire and forget you will just send messages and ignore all the errors.

To help the integration and error handling or logging, there is a variation of the flow where you can send data beside your notification.

## Scala only

You can build notification described in the original documentation.
It can be done by hand, or using some builder method.
See an example of the condition builder below.

Scala
: @@snip [snip](/huawei-push-kit/src/test/scala/docs/scaladsl/PushKitExamples.scala) { #condition-builder }
