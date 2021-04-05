# Google Cloud Logging

The Cloud Logging connector integrates with [Google Cloud Logging](https://cloud.google.com/logging/) to provide a logging backend for your application.
Note that at the moment it provides no support for directly interfacing with Cloud Logging APIs.

The loggers provided in this module are completely asynchronous and will never block your application.
To achieve this, if your application generates log events faster than they can be sent to the Cloud Logging service, they will be dropped according to their priority (i.e., first `TRACE` and `DEBUG`, then `INFO`, etc.).
The size of the priority queue used to buffer pending messages is fully configurable.

When running on Google Cloud services such as [Compute Engine](https://cloud.google.com/compute) or [Kubernetes Engine](https://cloud.google.com/kubernetes-engine), the Cloud Logging connector will automatically retrieve metadata from the environment and add it to your logs as resource labels.

@@project-info{ projectId="google-cloud-logging" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
group=com.lightbend.akka
artifact=akka-stream-alpakka-google-cloud-logging_$scala.binary.version$
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

The table below shows direct dependencies of this module and the second tab shows all libraries that it depends on transitively.

@@dependencies { projectId="google-cloud-logging" }

## Configuration

The Cloud Logging connector shares its @ref[basic configuration](google-common.md) with all the Google connectors in Alpakka.
The following sections describe additional configuration necessary to integrate with the logging framework of your choice.

## Akka Classic logging configuration

[Classic Logging](https://doc.akka.io/docs/akka/current/logging.html) is Akka’s native logging framework.
The @apidoc[CloudLogger] is fully configurable via the `application.conf` and automatically adds [MDC properties](https://doc.akka.io/docs/akka/current/logging.html#logging-thread-akka-source-and-actor-system-in-mdc) and [markers](https://doc.akka.io/docs/akka/current/logging.html#using-markers) as labels.

@@snip [snip](/google-cloud-logging/src/test/resources/application.conf) { #configure-classic-logging }
@@snip [snip](/google-cloud-logging/src/main/resources/reference.conf)

## Logback configuration

[Logback](http://logback.qos.ch/) is Akka’s [recommended](https://doc.akka.io/docs/akka/current/typed/logging.html#logback) SLF4J backend.

@@dependency [sbt,Maven,Gradle] {
group5=ch.qos.logback
artifact5=logback-classic
version5=1.2.3
}

Unfortunately the @apidoc[CloudLoggingAppender] cannot be configured in your `logback.xml` and must be initialized programmatically after the @apidoc[akka.actor.ActorSystem] has been created.
It is fully configurable via its setters and supports [MDC](http://logback.qos.ch/manual/mdc.html).

Scala
: @@snip [snip](/google-cloud-logging/src/test/scala/akka/stream/alpakka/googlecloud/logging/logback/CloudLoggingAppenderSpec.scala) { #logback-setup }

Java
: @@snip [snip](/google-cloud-logging/src/test/java/akka/stream/alpakka/googlecloud/logging/logback/CloudLoggingAppenderTest.java) { #logback-setup }
