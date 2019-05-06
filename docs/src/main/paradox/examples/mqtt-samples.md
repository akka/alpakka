# MQTT

## Example: Read from an MQTT topic, group messages and publish to Kafka

- (1) connection details to MQTT broker
- (2) settings for MQTT source specifying the topic to listen to
- (3) use helper method to cater for Paho failures on initial connect
- (4) add a kill switch to allow for stopping the subscription
- (5) convert incoming ByteString to String
- (6) parse JSON
- (7) group up to 50 messages into one, as long as they appear with 5 seconds
- (8) convert the list of measurements to a JSON array structure

Java
: @@snip [snip](/doc-examples/src/main/java/mqtt/javasamples/MqttGroupedWithin.java) { #flow }


### Restarting of the source

The MQTT source gets wrapped by a `RestartSource` to mitigate the 
@ref:[Paho initial connections problem](../mqtt.md#settings).

Java
: @@snip [snip](/doc-examples/src/main/java/mqtt/javasamples/MqttGroupedWithin.java) { #restarting }

### Json helper code

To use Java 8 time types (`Instant`) with Jackson, extra dependencies are required.

@@dependency [sbt,Maven,Gradle] {
  group1=com.fasterxml.jackson.datatype
  artifact1=jackson-datatype-jdk8
  version1=2.9.6
  group2=com.fasterxml.jackson.datatype
  artifact2=jackson-datatype-jsr310
  version2=2.9.6
}

Java
: @@snip [snip](/doc-examples/src/main/java/mqtt/javasamples/MqttGroupedWithin.java) { #json-mechanics }


### Running the example code

This example is contained in a stand-alone runnable main, it can be run
 from `sbt` like this:
 

sbt
:   ```
    sbt
    > doc-examples/run
    ```
