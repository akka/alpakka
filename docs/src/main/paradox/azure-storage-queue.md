# Azure Storage Queue Connector

The Azure Storage Queue connector provides an Akka Stream Source and Sinks for Azure Storage Queue integration.

Azure Storage Queue is a queuing service similar to Amazon's SQS. It is designed mostly for long-running and non-time-critical tasks. For more information on Azure Storage Queue see the [official docs](https://azure.microsoft.com/en-us/services/storage/queues/).

## Example usage

#### Init Azure Storage API

```scala
import com.microsoft.azure.storage._
import com.microsoft.azure.storage.queue._
val storageConnectionString = "DefaultEndpointsProtocol=http;AccountName=<YourAccountName>;AccountKey=<YourKey>"
val queueFactory = () => { // Since azure storage JDK is not guaranteed to be thread-safe
  val storageAccount = CloudStorageAccount.parse(storageConnectionString)
  val queueClient = storageAccount.createCloudQueueClient
  queueClient.getQueueReference("myQueue")
}
```
For more details, see [Microsoft Azure Storage Docs](https://docs.microsoft.com/en-us/azure/storage/storage-java-how-to-use-queue-storage).

#### Queuing a message
```scala
import one.aleph.akkzure.queue._
import one.aleph.akkzure.queue.scaladsl._

// Create an example message
val message = new CloudQueueMessage("Hello Azure")

Source.single(message).runWith(AzureQueueSink(queueFactory))
```

#### Processing and deleting messages
```scala
AzureQueueSource(queueFactory).take(10)
.map({ msg: CloudQueueMessage =>
  println(msg.getMessageContentAsString) // Print the messages content
  msg                                    // Return message to the flow for deletion
}).runWith(AzureQueueDeleteSink(queueFactory))
```