## Changelog

This is a fork of [alpakka](https://github.com/akka/alpakka) aimed at having better mqtt integration.

### v1.0-M1

#### Offline message buffering:

```scala
val offlinePersistenceSettings = MqttOfflinePersistenceSettings(
  bufferSize = 5000,
  deleteOldestMessage = false,
  persistBuffer = true
)

val connectionSettings = MqttConnectionSettings(
    "tcp://localhost:1883", 
    "test-scala-client", 
    new MemoryPersistence 
  )
  .withOfflinePersistenceSettings(
    offlinePersistenceSettings
  )
```


