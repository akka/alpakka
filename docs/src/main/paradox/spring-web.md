# Spring Web

Spring 5.0 introduced compatibility with [Reactive Streams](https://www.reactive-streams.org), a library interoperability standardization effort co-lead by Lightbend (with Akka Streams) along with Kaazing, Netflix, 
Pivotal, Red Hat, Twitter and many others.

Thanks to adopting Reactive Streams, multiple libraries can now inter-op since the same interfaces are implemented by 
all these libraries. Akka Streams by-design, hides the raw reactive-streams types from end-users, since it allows for
detaching these types from RS and allows for a painless migration to @javadoc[java.util.concurrent.Flow](java.util.concurrent.Flow) which was introduced in Java 9.

This Alpakka module makes it possible to directly return a `Source` in your Spring Web endpoints.

@@project-info{ projectId="spring-web" }


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
  artifact=akka-stream-alpakka-spring-web_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="spring-web" }


## Usage

Using Akka Streams in Spring Web (or Boot for that matter) is very simple, as Alpakka provides autoconfiguration to the
framework, which means that Spring is made aware of Sources and Sinks etc. 

All you need to do is include the above dependency (`akka-stream-alpakka-spring-web`), start your app as usual:

Java
: @@snip [snip](/spring-web/src/test/java/docs/javadsl/DemoApplication.java) { #use }


And you'll be able to return Akka Streams in HTTP endpoints directly:


Java
: @@snip [snip](/spring-web/src/test/java/docs/javadsl/SampleController.java) { #use }

Both `javadsl` and `scaladsl` Akka Stream types are supported.

In fact, since Akka supports Java 9 and the `java.util.concurrent.Flow.*` types already, before Spring, you could use it
to adapt those types in your applications as well.

### The provided configuration

The automatically enabled configuration is as follows:

Java
: @@snip [snip](/spring-web/src/main/java/akka/stream/alpakka/spring/web/SpringWebAkkaStreamsConfiguration.java) { #configure }

In case you'd like to manually configure it slightly differently.

## Shameless plug: Akka HTTP 

While the integration presented here works, it's not quite the optimal way of using Akka in conjunction with serving HTTP apps.
If you're new to reactive systems and picking technologies, you may want to have a look at @extref[Akka HTTP](akka-http:).

If, for some reason, you decided use Spring MVC this integration should help you achieve the basic streaming scenarios though.
