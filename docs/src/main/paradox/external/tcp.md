# TCP

Akka Streams provides a way of handling **TCP connections** with streams. While the general approach is very similar to the Actor based TCP handling using Akka IO, by using Akka Streams you are freed of having to manually react to back-pressure signals, as the library does it transparently for you.


## Akka TCP

Akka comes with its Reactive Streams-compliant TCP server and client.
Learn more about it in the [Akka Streaming TCP documentation](https://doc.akka.io/libraries/akka-core/current/stream/stream-io.html#streaming-tcp).           
