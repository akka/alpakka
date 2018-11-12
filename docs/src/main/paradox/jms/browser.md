# Browser

### Browsing messages from a JMS provider

The browse source will stream the messages in a queue without consuming them.

Create a source:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #create-browse-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #create-browse-source }

The `messageSelector` parameter can be used to filter the messages. Otherwise it will browse the entire content of the queue.

Unlike the other sources, the browse source will complete after browsing all the messages:

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #run-browse-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #run-browse-source }

**Notes:**

*  Messages may be arriving and expiring while the scan is done.
*  The JMS API does not require the content of an enumeration to be a static snapshot of queue content. Whether these changes are visible or not depends on the JMS provider.
*  A message must not be returned by a QueueBrowser before its delivery time has been reached.

