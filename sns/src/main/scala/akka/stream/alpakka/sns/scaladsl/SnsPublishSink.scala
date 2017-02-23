package akka.stream.alpakka.sns.scaladsl

import akka.Done
import akka.stream.alpakka.sns.SnsPublishSinkStage
import akka.stream.scaladsl.Sink
import com.amazonaws.services.sns.AmazonSNSAsync

import scala.concurrent.Future

object SnsPublishSink {

  /**
   * Scala API: creates a [[SnsPublishSinkStage]] for a SNS topic using an [[AmazonSNSAsync]]
   */
  def apply(topicArn: String)(implicit snsClient: AmazonSNSAsync): Sink[String, Future[Done]] =
    Sink.fromGraph(new SnsPublishSinkStage(topicArn, snsClient))

}
