/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import scala.language.postfixOps
import akka.stream.alpakka.pravega.scaladsl.Pravega
import akka.stream.scaladsl.{Sink, Source}
import io.pravega.client.stream.impl.UTF8StringSerializer

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

import io.pravega.client.stream.Serializer

import java.nio.ByteBuffer
import akka.stream.alpakka.testkit.scaladsl.Repeated

case class Auction(id: Int, description: String)
trait HasId {
  def id: Int
}
case class Player(id: Int, name: String, auctionId: Option[Int]) extends HasId
case class Winner(id: Int, name: String, auctionId: Int, description: String) extends HasId

class PravegaStreamAndTableSpec extends PravegaBaseSpec with Repeated {

  private val serializer = new UTF8StringSerializer

  private val intSerializer = new Serializer[Int] {
    override def serialize(value: Int): ByteBuffer = {
      val buff = ByteBuffer.allocate(4).putInt(value)
      buff.position(0)
      buff
    }

    override def deserialize(serializedValue: ByteBuffer): Int =
      serializedValue.getInt
  }

  private val familyExtractor: Any => String = _ => "test"

  private implicit val tablewriterSettings: TableWriterSettings[Int, String] =
    TableWriterSettingsBuilder[Int, String](system)
      .withSerializers(intSerializer, serializer)

  "Pravega stream and table" must {

    "work together" in {

      val fut = Source(1 to 100)
        .filter(_ % 2 == 0)
        .map(id => Auction(id = id, description = s"Auction N° $id"))
        .runWith(
          Pravega.tableSink(scope,
                            keyValueTableName,
                            (auction: Auction) => (auction.id, auction.description),
                            familyExtractor)
        )

      Await.ready(fut, 10 seconds)

      val readingDone = Source(100 to 200)
        .map(id => Player(id = id, name = s"Player $id", auctionId = Some(id % 100)))
        .via(
          Pravega
            .tableReadFlow(
              scope,
              keyValueTableName,
              (player: Player) => player.auctionId,
              (player: Player, description: Option[String]) =>
                description match {
                  case Some(value) =>
                    Winner(id = player.id,
                           name = player.name,
                           auctionId = player.auctionId.getOrElse(-1),
                           description = value)
                  case None => player
                },
              Some(familyExtractor)
            )
        )
        //        .wireTap(w => logger.info(w.toString()))
        .runWith(Sink.fold(0) { (sum, value) =>
          sum + value.id
        })

      whenReady(readingDone) { sum =>
        logger.info(s"Sum: $sum")
        sum mustEqual 15150
      }

    }

  }
}
