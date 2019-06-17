package akka.stream.alpakka.sqs.impl

import akka.stream.FlowShape
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Partition}
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import software.amazon.awssdk.services.sqs.model.{Message, ReceiveMessageRequest, ReceiveMessageResponse}

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._

class AutoBalancingSqsReceive(parallelism: Int)(implicit sqsClient: SqsAsyncClient) {
  private var isThrottling: Boolean = false

  def apply(): Flow[ReceiveMessageRequest, List[Message], _] = Flow.fromGraph(GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._

    val partition = b.add(Partition[ReceiveMessageRequest](2, _ => if (isThrottling) 1 else 0 ))
    val maxCapacity = b.add(Flow[ReceiveMessageRequest].mapAsyncUnordered(parallelism)(sqsClient.receiveMessage(_).toScala))
    val reducedCapacity = b.add(Flow[ReceiveMessageRequest].mapAsyncUnordered(1)(sqsClient.receiveMessage(_).toScala))
    val merge = b.add(Merge[ReceiveMessageResponse](2))
    val unwrapMessages = b.add(Flow[ReceiveMessageResponse].map(_.messages().asScala.toList))
    val control = b.add(Flow[List[Message]].map { messages =>
      isThrottling = messages.isEmpty
      messages
    })

    partition ~> maxCapacity     ~> merge ~> unwrapMessages ~> control
    partition ~> reducedCapacity ~> merge

    FlowShape(partition.in, control.out)
  })
}
