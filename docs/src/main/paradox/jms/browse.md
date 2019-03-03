# Browse

## Browsing messages

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



## Configure JMS browse

To connect to the JMS broker, first define an appropriate @javadoc[javax.jms.ConnectionFactory](javax.jms.ConnectionFactory). The Alpakka tests and all examples use Active MQ.

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsConnectorsSpec.scala) { #connection-factory }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsConnectorsTest.java) { #connection-factory }


The created @javadoc[ConnectionFactory](javax.jms.ConnectionFactory) is then used for the creation of the different JMS sources.


The `JmsBrowseSettings` factories allow for passing the actor system to read from the default  `alpakka.jms.browse` section, or you may pass a `Config` instance which is resolved to a section of the same structure. 

Scala
: @@snip [snip](/jms/src/test/scala/docs/scaladsl/JmsSettingsSpec.scala) { #browse-settings }

Java
: @@snip [snip](/jms/src/test/java/docs/javadsl/JmsSettingsTest.java) { #consumer-settings }


The Alpakka JMS browse soruce is configured via default settings in the [HOCON](https://github.com/lightbend/config#using-hocon-the-json-superset) config file section `alpakka.jms.browse` in your `application.conf`, and settings may be tweaked in the code using the `withXyz` methods. On the second tab the section from `reference.conf` shows the structure to use for configuring multiple set-ups.

Table
: Setting               | Description                                                          | Default Value       | 
------------------------|----------------------------------------------------------------------|---------------------|
connectionFactory       | Factory to use for creating JMS connections                          | Must be set in code |
destination             | The queue to browse                                                  | Must be set in code |
credentials             | JMS broker credentials                                               | Empty               |
connectionRetrySettings | Retry characteristics if the connection failed to be established or is taking a long time. | See @ref[Connection Retries](producer.md#connection-retries) 

reference.conf
: @@snip [snip](/jms/src/main/resources/reference.conf) { #browse }

