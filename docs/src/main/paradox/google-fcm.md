# Google Firebase Cloud Messaging

The google firebase cloud messaging connector provides a way to send notifications https://firebase.google.com/docs/cloud-messaging/ .

@@@ warning { title='Early state' }
The whole FCM server implementation doc is a bit unclear. 
This connector is build from scratch following the documentation.
So the error parsing, the condition builder, the apns object and some other object/case class
could (and possibly will) be improved.
@@@


### Reported issues

[Tagged issues at Github](https://github.com/akka/alpakka/labels/p%3Agoogle-fcm)


## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-google-fcm_$scalaBinaryVersion$
  version=$version$
}

## Usage

Possibly needed imports for the following codes.

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #imports }

Java
: @@snip ($alpakka$/google-fcm/src/test/java/akka/stream/alpakka/google/firebase/fcm/javadsl/Examples.java) { #imports }


Prepare the actor system and materializer.

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #init-mat }

Java
: @@snip ($alpakka$/google-fcm/src/test/java/akka/stream/alpakka/google/firebase/fcm/javadsl/Examples.java) { #init-mat }


Prepare your credentials for access to FCM.

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #init-credentials }

Java
: @@snip ($alpakka$/google-fcm/src/test/java/akka/stream/alpakka/google/firebase/fcm/javadsl/Examples.java) { #init-credentials }

The last two parameters in the above example are the predefined values. 
You can send test notifications [(so called validate only).](https://firebase.google.com/docs/reference/fcm/rest/v1/projects.messages/send)
And you can set the number of maximum concurrent connections.
There is a limitation in the docs; from one IP you can have maximum 1k pending connections, 
and you may need to configure `akka.http.host-connection-pool.max-open-requests` in your application.conf.

To send a notification message create your notification object, and send it!

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #simple-send }

Java
: @@snip ($alpakka$/google-fcm/src/test/java/akka/stream/alpakka/google/firebase/fcm/javadsl/Examples.java) { #simple-send }

With fire and forget you will just send messages and ignore all the errors.
This is not so healthy in general so you can use the send instead.

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #asFlow-send }

Java
: @@snip ($alpakka$/google-fcm/src/test/java/akka/stream/alpakka/google/firebase/fcm/javadsl/Examples.java) { #asFlow-send }

With this type of send you can get responses from the server.
These responses can be @scaladoc[positive](akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.FcmSuccessResponse) or @scaladoc[negative](akka.stream.alpakka.google.firebase.fcm.FcmFlowModels.FcmErrorResponse). 
You can choose what you want to do with this information, but keep in mind
if you try to resend the failed messages you will need to implement exponential backoff too!

To help the integration and error handling or logging, there is a variation of the flow where you can send data beside your notification.

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #withData-send }

Java
: @@snip ($alpakka$/google-fcm/src/test/java/akka/stream/alpakka/google/firebase/fcm/javadsl/Examples.java) { #withData-send }

Here I send a simple string, but you could use any type.

## Scala only

You can build any notification described in the original documentation.
It can be done by hand, or using some builder method.
If you build your notification from scratch with options (and not with the provided builders), worth to check isSendable before sending.

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #noti-create }

There is a condition builder too.

Scala
: @@snip ($alpakka$/google-fcm/src/test/scala/akka/stream/alpakka/google/firebase/fcm/scaladsl/Examples.scala) { #condition-builder }

## Running the examples

To run the example code you will need to configure a project and notifications in google firebase and provide your own credentials.
