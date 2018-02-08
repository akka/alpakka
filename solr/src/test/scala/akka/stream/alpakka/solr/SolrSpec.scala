/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.solr

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.solr.scaladsl.{SolrFlow, SolrSink, SolrSource}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestKit
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.embedded.JettyConfig
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider
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

  //#init-mat
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  //#init-mat
  //#init-client
  import org.apache.solr.client.solrj.impl.CloudSolrClient

  val zkHost = "127.0.0.1:9984/solr"
  implicit val client: SolrClient = new CloudSolrClient.Builder().withZkHost(zkHost).build
  //#init-client
  //#define-class
  case class Book(title: String)

  val bookToDoc: Book => SolrInputDocument = { b =>
    val doc = new SolrInputDocument
    doc.setField("title", b.title)
    doc
  }

  val tupleToBook: Tuple => Book = { t =>
    val title = t.getString("title")
    Book(title)
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
          IncomingMessage(doc)
        }
        .runWith(
          SolrSink.document(
            collection = "collection2",
            settings = SolrUpdateSettings(commitWithin = 5)
          )
        )
      //#run-document

      Await.result(f1, Duration.Inf)

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
          IncomingMessage(BookBean(title))
        }
        .runWith(
          SolrSink.bean[BookBean](
            collection = "collection3",
            settings = SolrUpdateSettings(commitWithin = 5)
          )
        )
      //#run-bean

      Await.result(res1, Duration.Inf)

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
          IncomingMessage(book)
        }
        .runWith(
          SolrSink
            .typed[Book](
              collection = "collection4",
              settings = SolrUpdateSettings(commitWithin = 5),
              binder = bookToDoc
            )
        )
      //#run-typed

      Await.result(res1, Duration.Inf)

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
          IncomingMessage(book)
        }
        .via(
          SolrFlow
            .typed[Book](
              collection = "collection5",
              settings = SolrUpdateSettings(commitWithin = 5),
              binder = bookToDoc
            )
        )
        .runWith(Sink.seq)
      //#run-flow

      val result1 = Await.result(res1, Duration.Inf)

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

      def commitToKakfa(offset: KafkaOffset): Unit =
        committedOffsets = committedOffsets :+ offset

      val res1 = Source(messagesFromKafka)
        .map { kafkaMessage: KafkaMessage =>
          val book = kafkaMessage.book
          println("title: " + book.title)

          // Transform message so that we can write to solr
          IncomingMessage(book, kafkaMessage.offset)
        }
        .via( // write to Solr
          SolrFlow.typedWithPassThrough[Book, KafkaOffset](
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
            commitToKakfa(result.passThrough)
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

      val result = Await.result(res2, Duration.Inf).toList

      result.sorted shouldEqual messagesFromKafka.map(_.book.title).sorted
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
    zkTestServer = new ZkTestServer(zkDir, 9984)
    zkTestServer.run()

    cluster = new MiniSolrCloudCluster(
      1,
      testWorkingDir.toPath,
      MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML,
      JettyConfig.builder.setContext("/solr").build,
      zkTestServer
    )
    cluster.getSolrClient.getClusterStateProvider
      .asInstanceOf[ZkClientClusterStateProvider]
      .uploadConfig(confDir.toPath, "conf")

    createCollection("collection1")

    assertTrue(!cluster.getSolrClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
  }

  private def createCollection(name: String) =
    CollectionAdminRequest
      .createCollection(name, "conf", 1, 1)
      .process(client)

  private def getTupleStream(collection: String): TupleStream = {
    //#tuple-stream
    val factory = new StreamFactory().withCollectionZkHost(collection, zkHost)
    val solrClientCache = new SolrClientCache()
    val streamContext = new StreamContext()
    streamContext.setSolrClientCache(solrClientCache)

    val expression = StreamExpressionParser.parse(s"""search($collection, q=*:*, fl="title", sort="title asc")""")
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
      SolrUpdateSettings(bufferSize = 10, retryInterval = 5000.millis, maxRetry = 100, commitWithin = -1)
    //#solr-update-settings
  }
}
