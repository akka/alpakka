# JMS

### Example: Read text messages from JMS queue and append to file

- listens to the JMS queue "test" receiving `String`s (1),
- converts incoming data to `akka.util.ByteString` (3),
- and appends the data to the file `target/out` (2).

Scala
: @@snip [snip](/doc-examples/src/main/scala/jms/JmsToFile.scala) { #sample }

Java
: @@snip [snip](/doc-examples/src/main/java/jms/javasamples/JmsToFile.java) { #sample }

### Example: Read text messages from JMS queue and create one file per message

- listens to the JMS queue "test" receiving `String`s (1),
- converts incoming data to `akka.util.ByteString` (2),
- combines the incoming data with a counter (3),
- creates an intermediary stream writing the incoming data to a file using the counter 
value to create unique file names (4). 

Scala
: @@snip [snip](/doc-examples/src/main/scala/jms/JmsToOneFilePerMessage.scala) { #sample }

Java
: @@snip [snip](/doc-examples/src/main/java/jms/javasamples/JmsToOneFilePerMessage.java) { #sample }

### Example: Read text messages from JMS queue and send to web server

- listens to the JMS queue "test" receiving `String`s (1),
- converts incoming data to `akka.util.ByteString` (2),
- puts the received text into an `HttpRequest` (3),
- sends the created request via Akka Http (4),
- prints the `HttpResponse` to standard out (5).

Scala
: @@snip [snip](/doc-examples/src/main/scala/jms/JmsToHttpGet.scala) { #sample }

Java
: @@snip [snip](/doc-examples/src/main/java/jms/javasamples/JmsToHttpGet.java) { #sample }

### Example: Read text messages from JMS queue and send to web socket

- listens to the JMS queue "test" receiving `String`s (1),
- configures a web socket flow to localhost (2),
- converts incoming data to a @scala[@scaladoc[ws.TextMessage](akka.http.scaladsl.model.ws.TextMessage)]@java[@scaladoc[akka.http.javadsl.model.ws.TextMessage](akka.http.javadsl.model.ws.TextMessage)] (3),
- pass the message via the web socket flow (4),
- convert the (potentially chunked) web socket reply to a `String` (5),
- prefix the `String` (6),
- end the stream by writing the values to standard out (7).

Scala
: @@snip [snip](/doc-examples/src/main/scala/jms/JmsToWebSocket.scala) { #sample }

Java
: @@snip [snip](/doc-examples/src/main/java/jms/javasamples/JmsToWebSocket.java) { #sample }

### Running the example code

This example is contained in a stand-alone runnable main, it can be run
 from `sbt` like this:
 

Scala
:   ```
    sbt
    > doc-examples/run
    ```
