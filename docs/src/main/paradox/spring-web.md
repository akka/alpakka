# Spring Web

Spring 5.0 introduced compatibility with [Reactive Streams](http://reactive-streams.org) 
, a library interoperability standardization effort co-lead by Lightbend (with Akka Streams) along with Kaazing, Netflix, Pivotal, Red Hat, Twitter and many others

Thanks to adopting reactive streams, multiple libraries can now inter-op since the same interfaces are implemented by 
all these libraries. Akka Streams by-design, hides the raw reactive-streams types from end-users, since it allows for
detaching these types from RS and allows for a painless migration to `java.util.concurrent.Flow` which was introduced in JDK9.

This Alpakka module makes it possible to directly return a `Source` in your Spring Web endpoints.

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-spring-web_$scalaBinaryVersion$
  version=$version$
}

## Usage

Usage is pretty simple, though likely could be improved if someone well-versed in the Spring ecosystem would look at it some more (using auto-configuration), and all you need to do is to register the `AkkaStreamsRegistrar` with Spring's internal Reactive Streams adapter:

Java
: @@snip (../../../../spring-web/src/test/java/akka/stream/alpakka/spring/web/MyAppConfiguration.java) { #configure }

Which will make Spring aware of Akka Streams types and their relation to their respective Reactive Streams types. From then on you can, for example, simply return a `Source `(regardless if it's `javadsl` or `scaladsl`) in an Spring mvc endpoint:
 
Java
: @@snip (../../../../spring-web/src/test/java/akka/stream/alpakka/spring/web/SampleController.java) { #use }



## Shameless plug: Akka HTTP 

While the integration presented here works, it's not quite the optimal way of using Akka in conjunction with serving HTTP apps.
If you're new to reactive systems and picking technologies, you may want to have a look at [Akka HTTP](https://doc.akka.io/docs/akka-http/current/scala/http/).

If, for some reason, you decided use Spring MVC this integration should help you achieve the basic streaming scenarios though.
