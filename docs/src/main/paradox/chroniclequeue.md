# OpenHFT Chronicle-Queue

The Chronicle Queue allows you to use Akka Streams Source, Sink and Flows backed by the persistent off-heap Chronicle queue.

@@project-info{ projectId="chronicle-queue" }

## Artifacts

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-chroniclequeue_$scala.binary.version$
  version=$project.version$
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="chronicle-queue" }


## Source

You can instatiate a Source from a chronicle queue:

Scala
: @@snip [snip](/chronicle-queue/src/test/scala/akka/stream/alpakka/chroniclequeue/ChronicleQueueSourceSinkSpec.scala) { #create-source }

## Sink

You can instatiate a Sink as a chronicle queue:

Scala
: @@snip [snip](/chronicle-queue/src/test/scala/akka/stream/alpakka/chroniclequeue/ChronicleQueueSourceSinkSpec.scala) { #create-sink }

## Flow

You can also use a Chronicle queue to back a Flow:

Scala
: @@snip [snip](/chronicle-queue/src/test/scala/akka/stream/alpakka/chroniclequeue/ChronicleQueueSpec.scala) { #create-buffer }

and use it with an async boundary:

Scala
: @@snip [snip](/chronicle-queue/src/test/scala/akka/stream/alpakka/chroniclequeue/ChronicleQueueSpec.scala) { #use-buffer }

## Running the example code

The code in this guide is part of runnable tests of this project. You are welcome to edit the code and run it in sbt.
