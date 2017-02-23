package akka.stream.alpakka.sns.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.sns.SnsPublishSinkStage
import akka.stream.javadsl.{Sink => JSink}
import akka.stream.scaladsl.Sink
import com.amazonaws.services.sns.AmazonSNSAsync

import scala.compat.java8.FutureConverters._

object SnsPublishSink {

  /**
   * Java API: creates a [[SnsPublishSinkStage]] for a SNS topic using an [[AmazonSNSAsync]]
   */
  def create(topicArn: String, snsClient: AmazonSNSAsync): JSink[String, CompletionStage[Done]] = {
    val sink = Sink.fromGraph(new SnsPublishSinkStage(topicArn, snsClient))
    sink.mapMaterializedValue(_.toJava).asJava
  }

}
