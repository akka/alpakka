/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega

import akka.stream.{KillSwitches, SourceShape}

import akka.stream.scaladsl.{Broadcast, GraphDSL, Keep, Sink, Source, Zip}

import akka.stream.alpakka.testkit.scaladsl.Repeated
import akka.stream.alpakka.pravega.scaladsl.{Pravega, PravegaTable}

import scala.concurrent.{Await, Promise}
import scala.util.Using
import io.pravega.client.tables.TableKey

import docs.scaladsl.Serializers._

class PravegaStreamAndTableSpec extends PravegaBaseSpec with Repeated {

  val writterSettings = WriterSettingsBuilder(system).withSerializer(stringSerializer)

  val readerSettings = ReaderSettingsBuilder(system)
    .withSerializer(stringSerializer)

  def readTableFlow(scope: String, tableName: String) = {
    // #table-reading-flow
    val tableSettings: TableSettings[String, Int] =
      TableReaderSettingsBuilder[String, Int]()
        .withTableKey(p => new TableKey(stringSerializer.serialize(p)))
        .build()
    PravegaTable
      .readFlow[String, Int](
        scope,
        tableName,
        tableSettings
      )
    // #table-reading-flow

  }

  def writeTableFlow(scope: String, tableName: String) = {
    val tableWriterSettings: TableWriterSettings[String, Int] =
      TableWriterSettingsBuilder[String, Int]()
        .withSerializers(k => new TableKey(stringSerializer.serialize(k)))
        .build()
    PravegaTable.writeFlow(
      scope,
      tableName,
      tableWriterSettings
    )
  }

  "Pravega table" must {

    "allow stateful streaming" in {

      val scope = newScope()
      val stream = "my-stream"
      val tableName = "my-table"
      val groupName = "my-group"

      createStream(scope, stream)
      createTable(scope, tableName, 6)

      Using(Pravega.readerGroupManager(scope, readerSettings.clientConfig)) { readerGroupManager =>
        readerGroupManager.createReaderGroup(groupName, stream)
      }.foreach { readerGroup =>
        val source = Pravega.source(readerGroup, readerSettings).map(_.message)

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

        val sink1 = Pravega.sink(scope, stream, writterSettings)

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

        Await.ready(finishReading.future, remainingOrDefault)

        kill.shutdown()

        whenReady(res) { res =>
          res mustBe empty
        }

      }
    }

  }
}
