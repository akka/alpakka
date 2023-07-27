# Azure Storage Queue

The Azure Storage Queue connector provides an Akka Stream Source and Sinks for Azure Storage Queue integration.

Azure Storage Queue is a queuing service similar to Amazon's SQS. It is designed mostly for long-running and non-time-critical tasks. For more information on Azure Storage Queue see the [Azure docs](https://azure.microsoft.com/en-us/services/storage/queues/).

@@project-info{ projectId="azure-storage-queue" }

## Artifacts

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

Additionally, add the dependencies as below.

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-stream-alpakka-azure-storage-queue_$scala.binary.version$
  version=$project.version$
  symbol2=AkkaVersion
  value2=$akka.version$
  group2=com.typesafe.akka
  artifact2=akka-stream_$scala.binary.version$
  version2=AkkaVersion
}

The table below shows direct dependencies of this module and the second tab shows all libraries it depends on transitively.

@@dependencies { projectId="azure-storage-queue" }


## Init Azure Storage API

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

For more details, see [Microsoft Azure Storage Docs](https://docs.microsoft.com/en-us/azure/storage/queues/storage-java-how-to-use-queue-storage).

## Queuing a message

```scala
import one.aleph.akkzure.queue._
import one.aleph.akkzure.queue.scaladsl._

// Create an example message
val message = new CloudQueueMessage("Hello Azure")

Source.single(message).runWith(AzureQueueSink(queueFactory))
```

## Processing and deleting messages

```scala
AzureQueueSource(queueFactory).take(10)
.map({ msg: CloudQueueMessage =>
  println(msg.getMessageContentAsString) // Print the messages content
  msg                                    // Return message to the flow for deletion
}).runWith(AzureQueueDeleteSink(queueFactory))
```
