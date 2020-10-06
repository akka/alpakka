/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import akka.stream.{KillSwitches, SourceShape}

import akka.stream.scaladsl.{Broadcast, GraphDSL, Keep, Sink, Source, Zip}
import io.pravega.client.stream.impl.UTF8StringSerializer
import io.pravega.client.stream.Serializer

import java.nio.ByteBuffer
import akka.stream.alpakka.testkit.scaladsl.Repeated
import akka.stream.alpakka.pravega.scaladsl.{Pravega, PravegaTable}

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Promise}
import scala.util.Using

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

  implicit val writterSettings = WriterSettingsBuilder(system).withSerializer(serializer)

  private val familyExtractor: String => String =
    _ => "test"

  implicit val readerSettings = ReaderSettingsBuilder(system)
    .withSerializer(new UTF8StringSerializer)

  def readTableFlow(scope: String, tableName: String) = {
    // #table-reading-flow
    implicit val tableSettings: TableSettings[String, Int] =
      TableWriterSettingsBuilder[String, Int](system)
        .withSerializers(serializer, intSerializer)
    PravegaTable
      .readFlow[String, Int](
        scope,
        tableName,
        familyExtractor
      )
    // #table-reading-flow

  }

  def writeTableFlow(scope: String, tableName: String) = {
    implicit val tableWriteSettings: TableWriterSettings[String, Int] =
      TableWriterSettingsBuilder[String, Int](system)
        .withSerializers(serializer, intSerializer)
    PravegaTable.writeFlow(
      scope,
      tableName,
      familyExtractor
    )
  }

  "Pravega table" must {

    "allow stateful streaming" in {

      val scope = newScope()
      val stream = "my-stream"
      val tableName = "my-table"
      val groupName = "my-group"

      createStream(scope, stream)
      createTable(scope, tableName)

      Using(Pravega.readerGroupManager(scope, readerSettings.clientConfig)) { readerGroupManager =>
        readerGroupManager.createReaderGroup(groupName, stream)
      }.foreach { readerGroup =>
        val source = Pravega.source(readerGroup).map(_.message)

        val source2 = Source.fromGraph(GraphDSL.create() { implicit builder =>
          import GraphDSL.Implicits._

          val bcast = builder.add(Broadcast[String](2))
          val zip = builder.add(Zip[String, Int]())

          val lookup = readTableFlow(scope, tableName)
            .map {
              case None => 1
              case Some(value) => value + 1
            }
          val writeFlow = writeTableFlow(scope, tableName)

          val increment = builder.add(writeFlow)

          // format: off

          source  ~> bcast      ~>        zip.in0
                                          zip.out   ~> increment
                     bcast  ~> lookup ~>  zip.in1


          // format: on
          SourceShape(increment.out)
        })

        val sink1 = Pravega.sink(scope, stream)

        Source(1 to 100)
          .map(_ % 10)
          .map(id => s"name_$id")
          .runWith(sink1)

        val finishReading = Promise[Unit]()

        val count = (0 to 9).map(id => (s"name_$id", 10)).toMap

        val (kill, res) = source2
          .filter(_._2 == 10)
          .viaMat(KillSwitches.single)(Keep.right)
          .toMat(Sink.fold(count) {
            case (acc, (name, _)) =>
              val it = acc - name
              if (it.isEmpty)
                finishReading.success(())
              it
          })(Keep.both)
          .run()

        Await.ready(finishReading.future, 10.seconds)

        kill.shutdown()

        whenReady(res) { res =>
          res mustBe empty
        }

      }
    }

  }
}
