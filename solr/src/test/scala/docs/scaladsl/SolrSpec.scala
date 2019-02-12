/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import java.io.File
import java.util.{Arrays, Optional}
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.alpakka.solr._
import akka.stream.alpakka.solr.scaladsl.{SolrFlow, SolrSink, SolrSource}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.apache.solr.client.solrj.embedded.JettyConfig
import org.apache.solr.client.solrj.impl.{CloudSolrClient, ZkClientClusterStateProvider}
import org.apache.solr.client.solrj.io.stream.expr.{StreamExpressionParser, StreamFactory}
import org.apache.solr.client.solrj.io.stream.{CloudSolrStream, StreamContext, TupleStream}
import org.apache.solr.client.solrj.io.{SolrClientCache, Tuple}
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, UpdateRequest}
import org.apache.solr.cloud.{MiniSolrCloudCluster, ZkTestServer}
import org.apache.solr.common.SolrInputDocument
import org.junit.Assert.assertTrue
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
import scala.concurrent.duration._

class SolrSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  private var cluster: MiniSolrCloudCluster = _

  private var zkTestServer: ZkTestServer = _
  implicit val system: ActorSystem = ActorSystem()

  implicit val materializer: Materializer = ActorMaterializer()
  //#init-client
  final val zookeeperPort = 9984
  final val zkHost = s"127.0.0.1:$zookeeperPort/solr"
  implicit val client: CloudSolrClient = new CloudSolrClient.Builder(Arrays.asList(zkHost), Optional.empty()).build

  //#init-client
  //#define-class
  case class Book(title: String, comment: String = "", routerOpt: Option[String] = None)

  val bookToDoc: Book => SolrInputDocument = { b =>
    val doc = new SolrInputDocument
    doc.setField("title", b.title)
    doc.setField("comment", b.comment)
    b.routerOpt.foreach { router =>
      doc.setField("router", router)
    }
    doc
  }

  val tupleToBook: Tuple => Book = { t =>
    val title = t.getString("title")
    Book(title, t.getString("comment"))
  }
  //#define-class

  "Un-typed Solr connector" should {
    "consume and publish SolrInputDocument" in {
      // Copy collection1 to collection2 through document stream
      createCollection("collection2") //create a new collection
      val stream = getTupleStream("collection1")

      //#run-document
      val f1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.documents(
            collection = "collection2",
            settings = SolrUpdateSettings(commitWithin = 5)
          )
        )
      //#run-document

      Await.result(f1, Duration.Inf)

      client.commit("collection2")

      val stream2 = getTupleStream("collection2")

      val res2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

  }

  "Typed Solr connector" should {
    "consume and publish documents as specific type using a bean" in {
      // Copy collection1 to collection3 through bean stream
      createCollection("collection3") //create a new collection
      val stream = getTupleStream("collection1")

      //#define-bean
      import org.apache.solr.client.solrj.beans.Field

      import scala.annotation.meta.field
      case class BookBean(@(Field @field) title: String)
      //#define-bean

      //#run-bean
      val res1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val title = tuple.getString("title")
          WriteMessage.createUpsertMessage(BookBean(title))
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.beans[BookBean](
            collection = "collection3",
            settings = SolrUpdateSettings(commitWithin = 5)
          )
        )
      //#run-bean

      Await.result(res1, Duration.Inf)

      client.commit("collection3")

      val stream2 = getTupleStream("collection3")

      val res2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "Typed Solr connector" should {
    "consume and publish documents as specific type with a binder" in {
      // Copy collection1 to collection4 through typed stream
      createCollection("collection4") //create a new collection
      val stream = getTupleStream("collection1")

      //#run-typed
      val res1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink
            .typeds[Book](
              collection = "collection4",
              settings = SolrUpdateSettings(commitWithin = 5),
              binder = bookToDoc
            )
        )
      //#run-typed

      Await.result(res1, Duration.Inf)

      client.commit("collection4")

      val stream2 = getTupleStream("collection4")

      val res2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "SolrFlow" should {
    "store documents and pass status to downstream" in {
      // Copy collection1 to collection5 through typed stream
      createCollection("collection5") //create a new collection
      val stream = getTupleStream("collection1")

      //#run-flow
      val res1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .via(
          SolrFlow
            .typeds[Book](
              collection = "collection5",
              settings = SolrUpdateSettings(commitWithin = 5),
              binder = bookToDoc
            )
        )
        .runWith(Sink.seq)
      //#run-flow

      val result1 = Await.result(res1, Duration.Inf)

      client.commit("collection5")

      // Assert no errors
      assert(result1.forall(_.exists(_.status == 0)))

      val stream2 = getTupleStream("collection5")

      val res2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }
  }

  "SolrFlow" should {
    "kafka-example - store documents and pass responses with passThrough" in {
      createCollection("collection6") //create new collection

      //#kafka-example
      // We're going to pretend we got messages from kafka.
      // After we've written them to Solr, we want
      // to commit the offset to Kafka

      case class KafkaOffset(offset: Int)
      case class KafkaMessage(book: Book, offset: KafkaOffset)

      val messagesFromKafka = List(
        KafkaMessage(Book("Book 1"), KafkaOffset(0)),
        KafkaMessage(Book("Book 2"), KafkaOffset(1)),
        KafkaMessage(Book("Book 3"), KafkaOffset(2))
      )

      var committedOffsets = List[KafkaOffset]()

      def commitToKafka(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val res1 = Source(messagesFromKafka)
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          println("title: " + book.title)

          // Transform message so that we can write to solr
          WriteMessage.createUpsertMessage(book).withPassThrough(kafkaMessage.offset)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .via( // write to Solr
          SolrFlow.typedsWithPassThrough[Book, KafkaOffset](
            collection = "collection6",
            settings = SolrUpdateSettings(commitWithin = 5),
            binder = bookToDoc
          )
        )
        .map { messageResults =>
          messageResults.foreach { result =>
            if (result.status != 0)
              throw new Exception("Failed to write message to Solr")
            // Commit to kafka
            commitToKafka(result.passThrough)
          }
        }
        .runWith(Sink.ignore)
      //#kafka-example

      Await.ready(res1, Duration.Inf)

      // Make sure all messages was committed to kafka
      assert(List(0, 1, 2) == committedOffsets.map(_.offset))

      val stream = getTupleStream("collection6")

      val res2 = SolrSource
        .fromTupleStream(stream)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result.sorted shouldEqual messagesFromKafka.map(_.book.title).sorted
    }
  }

  "Un-typed Solr connector" should {
    "consume and delete documents" in {
      // Copy collection1 to collection2 through document stream
      createCollection("collection7") //create a new collection
      val stream = getTupleStream("collection1")

      val f1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.documents(
            collection = "collection7",
            settings = SolrUpdateSettings(commitWithin = 5)
          )
        )

      Await.result(f1, Duration.Inf)

      client.commit("collection7")

      val stream2 = getTupleStream("collection7")

      //#delete-documents
      val f2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map { tuple: Tuple =>
          WriteMessage.createDeleteMessage[SolrInputDocument](tuple.fields.get("title").toString)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.documents(
            collection = "collection7",
            settings = SolrUpdateSettings()
          )
        )
      //#delete-documents

      Await.result(f2, Duration.Inf)

      client.commit("collection7")

      val stream3 = getTupleStream("collection7")

      val res2 = SolrSource
        .fromTupleStream(ts = stream3)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq.empty[String]
    }

  }

  "Un-typed Solr connector" should {
    "consume and update atomically documents" in {
      // Copy collection1 to collection2 through document stream
      createCollection("collection8") //create a new collection
      val stream = getTupleStream("collection1")

      val f1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple).copy(comment = "Written by good authors.")
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.documents(
            collection = "collection8",
            settings = SolrUpdateSettings(commitWithin = 5)
          )
        )

      Await.result(f1, Duration.Inf)

      client.commit("collection8")

      val stream2 = getTupleStream("collection8")

      //#update-atomically-documents
      val f2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map { tuple: Tuple =>
          WriteMessage.createUpdateMessage[SolrInputDocument](
            "title",
            tuple.fields.get("title").toString,
            Map("comment" -> Map("set" -> (tuple.fields.get("comment") + " It is a good book!!!")))
          )
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.documents(
            collection = "collection8",
            settings = SolrUpdateSettings()
          )
        )
      //#update-atomically-documents

      Await.result(f2, Duration.Inf)

      client.commit("collection8")

      val stream3 = getTupleStream("collection8")

      val res2 = SolrSource
        .fromTupleStream(ts = stream3)
        .map(tupleToBook)
        .map { b =>
          b.title + ". " + b.comment
        }
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency. Written by good authors. It is a good book!!!",
        "Akka in Action. Written by good authors. It is a good book!!!",
        "Effective Akka. Written by good authors. It is a good book!!!",
        "Learning Scala. Written by good authors. It is a good book!!!",
        "Programming in Scala. Written by good authors. It is a good book!!!",
        "Scala Puzzlers. Written by good authors. It is a good book!!!",
        "Scala for Spark in Production. Written by good authors. It is a good book!!!"
      )
    }

  }

  "Solr connector" should {
    "consume and delete beans" in {
      // Copy collection1 to collection2 through document stream
      createCollection("collection9") //create a new collection
      val stream = getTupleStream("collection1")

      val f1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.typeds[Book](
            collection = "collection9",
            settings = SolrUpdateSettings(commitWithin = 5),
            binder = bookToDoc
          )
        )

      Await.result(f1, Duration.Inf)

      client.commit("collection9")

      val stream2 = getTupleStream("collection9")

      val f2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map { tuple: Tuple =>
          WriteMessage.createDeleteMessage[Book](tuple.fields.get("title").toString)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.typeds[Book](
            collection = "collection9",
            settings = SolrUpdateSettings(),
            binder = bookToDoc
          )
        )

      Await.result(f2, Duration.Inf)

      client.commit("collection9")

      val stream3 = getTupleStream("collection9")

      val res2 = SolrSource
        .fromTupleStream(ts = stream3)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq.empty[String]
    }

  }

  "Solr connector" should {
    "consume and update atomically beans" in {
      // Copy collection1 to collection2 through document stream
      createCollection("collection10", Some("router")) //create a new collection
      val stream = getTupleStream("collection1")

      val f1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book =
            tupleToBook(tuple).copy(comment = "Written by good authors.", routerOpt = Some("router-value"))
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.typeds[Book](
            collection = "collection10",
            settings = SolrUpdateSettings(commitWithin = 5),
            binder = bookToDoc
          )
        )

      Await.result(f1, Duration.Inf)

      client.commit("collection10")

      val stream2 = getTupleStream("collection10")

      val f2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map { tuple: Tuple =>
          WriteMessage.createUpdateMessage[Book](
            idField = "title",
            tuple.fields.get("title").toString,
            routingFieldValue = Some("router-value"),
            updates = Map("comment" -> Map("set" -> (tuple.fields.get("comment") + " It is a good book!!!")))
          )
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.typeds[Book](
            collection = "collection10",
            settings = SolrUpdateSettings(commitWithin = 5),
            binder = bookToDoc
          )
        )

      Await.result(f2, Duration.Inf)

      client.commit("collection10")

      val stream3 = getTupleStream("collection10")

      val res2 = SolrSource
        .fromTupleStream(ts = stream3)
        .map(tupleToBook)
        .map { b =>
          b.title + ". " + b.comment
        }
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq(
        "Akka Concurrency. Written by good authors. It is a good book!!!",
        "Akka in Action. Written by good authors. It is a good book!!!",
        "Effective Akka. Written by good authors. It is a good book!!!",
        "Learning Scala. Written by good authors. It is a good book!!!",
        "Programming in Scala. Written by good authors. It is a good book!!!",
        "Scala Puzzlers. Written by good authors. It is a good book!!!",
        "Scala for Spark in Production. Written by good authors. It is a good book!!!"
      )
    }

  }

  "Un-typed Solr connector" should {
    "consume and delete documents by query" in {
      // Copy collection1 to collection2 through document stream
      createCollection("collection11") //create a new collection
      val stream = getTupleStream("collection1")

      val f1 = SolrSource
        .fromTupleStream(ts = stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.documents(
            collection = "collection11",
            settings = SolrUpdateSettings(commitWithin = 5)
          )
        )

      Await.result(f1, Duration.Inf)

      client.commit("collection11")

      val stream2 = getTupleStream("collection11")

      //#delete-documents-query
      val f2 = SolrSource
        .fromTupleStream(ts = stream2)
        .map { tuple: Tuple =>
          WriteMessage.createDeleteByQueryMessage[SolrInputDocument](
            "title:\"" + tuple.fields.get("title").toString + "\""
          )
        }
        .groupedWithin(5, new FiniteDuration(10, TimeUnit.MILLISECONDS))
        .runWith(
          SolrSink.documents(
            collection = "collection11",
            settings = SolrUpdateSettings()
          )
        )
      //#delete-documents-query

      Await.result(f2, Duration.Inf)

      client.commit("collection11")

      val stream3 = getTupleStream("collection11")

      val res2 = SolrSource
        .fromTupleStream(ts = stream3)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      val result = Await.result(res2, Duration.Inf)

      result shouldEqual Seq.empty[String]
    }

  }

  override def beforeAll(): Unit = {
    setupCluster()
    new UpdateRequest()
      .add("title", "Akka in Action")
      .add("title", "Programming in Scala")
      .add("title", "Learning Scala")
      .add("title", "Scala for Spark in Production")
      .add("title", "Scala Puzzlers")
      .add("title", "Effective Akka")
      .add("title", "Akka Concurrency")
      .commit(client, "collection1")
  }

  override def afterAll(): Unit = {
    client.close()
    cluster.shutdown()
    zkTestServer.shutdown()
    TestKit.shutdownActorSystem(system)
  }

  private def setupCluster(): Unit = {
    val targetDir = new File("solr/target")
    val testWorkingDir =
      new File(targetDir, "scala-solr-" + System.currentTimeMillis)
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs

    val confDir = new File("solr/src/test/resources/conf")

    val zkDir = testWorkingDir.toPath.resolve("zookeeper/server/data").toString
    zkTestServer = new ZkTestServer(zkDir, zookeeperPort)
    zkTestServer.run()

    cluster = new MiniSolrCloudCluster(
      1,
      testWorkingDir.toPath,
      MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML,
      JettyConfig.builder.setContext("/solr").build,
      zkTestServer
    )
    client.getClusterStateProvider
      .asInstanceOf[ZkClientClusterStateProvider]
      .uploadConfig(confDir.toPath, "conf")
    client.setIdField("router")

    createCollection("collection1")

    assertTrue(!client.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
  }

  private def createCollection(name: String, routerFieldOpt: Option[String] = None) =
    CollectionAdminRequest
      .createCollection(name, "conf", 1, 1)
      .setRouterField(routerFieldOpt.orNull)
      .process(client)

  private def getTupleStream(collection: String): TupleStream = {
    //#tuple-stream
    val factory = new StreamFactory().withCollectionZkHost(collection, zkHost)
    val solrClientCache = new SolrClientCache()
    val streamContext = new StreamContext()
    streamContext.setSolrClientCache(solrClientCache)

    val expression =
      StreamExpressionParser.parse(s"""search($collection, q=*:*, fl="title,comment", sort="title asc")""")
    val stream: TupleStream = new CloudSolrStream(expression, factory)
    stream.setStreamContext(streamContext)
    //#tuple-stream
    stream
  }

  private def documentation: Unit = {
    val stream: TupleStream = null
    //#define-source
    val source = SolrSource
      .fromTupleStream(ts = stream)
    //#define-source
    //#solr-update-settings
    import akka.stream.alpakka.solr.SolrUpdateSettings

    val settings =
      SolrUpdateSettings(commitWithin = -1)
    //#solr-update-settings
  }
}
