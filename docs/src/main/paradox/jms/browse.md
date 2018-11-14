# Browse

### Browsing messages

The browse source streams the messages in a queue **without consuming them**.

Unlike the other sources, the browse source will complete after browsing all the messages currently on the queue.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #browse-source }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #browse-source }

A JMS `selector` can be used to filter the messages. Otherwise it will browse the entire content of the queue.


**Notes:**

*  Messages may be arriving and expiring while the scan is done.
*  The JMS API does not require the content of an enumeration to be a static snapshot of queue content. Whether these changes are visible or not depends on the JMS provider.
*  A message must not be returned by a QueueBrowser before its delivery time has been reached.
