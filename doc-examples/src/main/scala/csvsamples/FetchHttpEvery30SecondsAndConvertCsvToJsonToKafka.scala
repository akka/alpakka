/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package csvsamples

// #sample
import akka.http.scaladsl._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.util.ByteString
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{
  ByteArrayDeserializer,
  ByteArraySerializer,
  StringDeserializer,
  StringSerializer
}
import playground.ActorSystemAvailable
import spray.json.{DefaultJsonProtocol, JsValue, JsonWriter}

import scala.concurrent.duration.DurationInt
// #sample

object FetchHttpEvery30SecondsAndConvertCsvToJsonToKafka
    extends ActorSystemAvailable
    with App
    with DefaultJsonProtocol {

  // format: off
  // #helper
  val httpRequest = HttpRequest(uri = "http://www.nasdaq.com/screening/companies-by-name.aspx?exchange=NASDAQ&render=download")

  def extractEntityData(response: HttpResponse): Source[ByteString, _] =
    response match {
      case HttpResponse(OK, _, entity, _) => entity.dataBytes
      case notOkResponse =>
        Source.failed(new RuntimeException(s"illegal response $notOkResponse"))
    }

  def cleanseCsvData(csvData: Map[String, ByteString]): Map[String, String] =
    csvData
      .filterNot { case (key, _) => key.isEmpty }
      .mapValues(_.utf8String)

  def toJson(map: Map[String, String])(
      implicit jsWriter: JsonWriter[Map[String, String]]): JsValue = jsWriter.write(map)
  // #helper
  // format: on

  val kafkaPort = 19000
  EmbeddedKafka.start()(EmbeddedKafkaConfig(kafkaPort))

  val kafkaProducerSettings = ProducerSettings(actorSystem, new ByteArraySerializer, new StringSerializer)
    .withBootstrapServers(s"localhost:$kafkaPort")

  val (ticks, future) =
    // format: off
    // #sample

    Source                                                         // stream element type
      .tick(1.seconds, 30.seconds, httpRequest)                    //: HttpRequest             (1)
      .mapAsync(1)(Http().singleRequest(_))                        //: HttpResponse            (2)
      .flatMapConcat(extractEntityData)                            //: ByteString              (3)
      .via(CsvParsing.lineScanner())                               //: List[ByteString]        (4)
      .via(CsvToMap.toMap())                                       //: Map[String, ByteString] (5)
      .map(cleanseCsvData)                                         //: Map[String, String]     (6)
      .map(toJson)                                                 //: JsValue                 (7)
      .map(_.compactPrint)                                         //: String (JSON formatted)
      .map { elem =>
        new ProducerRecord[Array[Byte], String]("topic1", elem)    //: Kafka ProducerRecord
      }
      .toMat(Producer.plainSink(kafkaProducerSettings))(Keep.both)
      .run()
      // #sample
      // format: on

  val kafkaConsumerSettings = ConsumerSettings(actorSystem, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers(s"localhost:$kafkaPort")
    .withGroupId("topic1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val done = Consumer
    .atMostOnceSource(kafkaConsumerSettings, Subscriptions.topics("topic1"))
    .map(_.value)
    .runWith(Sink.foreach(println))

  wait(1.minutes)
  ticks.cancel()
  EmbeddedKafka.stop()
  terminateActorSystem()
}
