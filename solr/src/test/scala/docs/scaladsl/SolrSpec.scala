/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.scaladsl

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Arrays, Optional}

import akka.Done
import akka.actor.ActorSystem
import akka.stream.alpakka.solr._
import akka.stream.alpakka.solr.scaladsl.{SolrFlow, SolrSink, SolrSource}
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
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
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.BeforeAndAfterAll

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SolrSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll with ScalaFutures with LogCapturing {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds)

  private var cluster: MiniSolrCloudCluster = _

  private var zkTestServer: ZkTestServer = _
  implicit val system: ActorSystem = ActorSystem()
  implicit val commitExecutionContext: ExecutionContext = ExecutionContext.global

  final val predefinedCollection = "collection1"

  //#init-client
  final val zookeeperPort = 9984
  final val zookeeperHost = s"127.0.0.1:$zookeeperPort/solr"
  implicit val solrClient: CloudSolrClient =
    new CloudSolrClient.Builder(Arrays.asList(zookeeperHost), Optional.empty()).build

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

  "Alpakka Solr" should {
    "consume and publish SolrInputDocument" in {
      // Copy collection1 to collectionName through document stream
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      //#run-document
      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.documents(collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)
      //#run-document

      copyCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "consume and publish documents as specific type using a bean" in {
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      //#define-bean
      import org.apache.solr.client.solrj.beans.Field

      import scala.annotation.meta.field
      case class BookBean(@(Field @field) title: String)
      //#define-bean

      //#run-bean
      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val title = tuple.getString("title")
          WriteMessage.createUpsertMessage(BookBean(title))
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.beans[BookBean](collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)
      //#run-bean

      copyCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "consume and publish documents as specific type with a binder" in {
      // Copy collection1 to collection4 through typed stream
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      //#run-typed
      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink
            .typeds[Book](
              collectionName,
              SolrUpdateSettings(),
              binder = bookToDoc
            )
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)
      //#run-typed

      copyCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "store documents and pass status to downstream" in {
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      // #typeds-flow
      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, 10.millis)
        .via(
          SolrFlow
            .typeds[Book](
              collectionName,
              SolrUpdateSettings(),
              binder = bookToDoc
            )
        )
        .runWith(Sink.seq)
        // explicit commit when stream ended
        .map { seq =>
          solrClient.commit(collectionName)
          seq
        }(commitExecutionContext)
      // #typeds-flow

      val result1 = copyCollection.futureValue

      // Assert no errors
      assert(result1.forall(_.exists(_.status == 0)))

      val stream2 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream2)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq(
        "Akka Concurrency",
        "Akka in Action",
        "Effective Akka",
        "Learning Scala",
        "Programming in Scala",
        "Scala Puzzlers",
        "Scala for Spark in Production"
      )
    }

    "store documents and pass responses with passThrough (Kafka example)" in {
      val collectionName = createCollection()

      var committedOffsets = List[CommittableOffset]()

      case class CommittableOffset(offset: Int) {
        def commitScaladsl(): Future[Done] = {
          committedOffsets = committedOffsets :+ this
          Future.successful(Done)
        }
      }

      case class CommittableOffsetBatch(offsets: immutable.Seq[CommittableOffset]) {
        def commitScaladsl(): Future[Done] = {
          committedOffsets = committedOffsets ++ offsets
          Future.successful(Done)
        }
      }

      case class CommittableMessage(book: Book, committableOffset: CommittableOffset)

      val messagesFromKafka = List(
        CommittableMessage(Book("Book 1"), CommittableOffset(0)),
        CommittableMessage(Book("Book 2"), CommittableOffset(1)),
        CommittableMessage(Book("Book 3"), CommittableOffset(2))
      )
      val kafkaConsumerSource = Source(messagesFromKafka)
      //#kafka-example
      // Note: This code mimics Alpakka Kafka APIs
      val copyCollection = kafkaConsumerSource
        .map { kafkaMessage: CommittableMessage =>
          val book = kafkaMessage.book
          // Transform message so that we can write to solr
          WriteMessage.createUpsertMessage(book).withPassThrough(kafkaMessage.committableOffset)
        }
        .groupedWithin(5, 10.millis)
        .via( // write to Solr
          SolrFlow.typedsWithPassThrough[Book, CommittableOffset](
            collectionName,
            // use implicit commits to Solr
            SolrUpdateSettings().withCommitWithin(5),
            binder = bookToDoc
          )
        ) // check status and collect Kafka offsets
        .map { messageResults =>
          val offsets = messageResults.map { result =>
            if (result.status != 0)
              throw new Exception("Failed to write message to Solr")
            result.passThrough
          }
          CommittableOffsetBatch(offsets)
        }
        .mapAsync(1)(_.commitScaladsl())
        .runWith(Sink.ignore)
      //#kafka-example

      copyCollection.futureValue

      // Make sure all messages was committed to kafka
      assert(List(0, 1, 2) == committedOffsets.map(_.offset))

      val stream = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue.sorted shouldEqual messagesFromKafka.map(_.book.title).sorted
    }

    "consume and delete documents" in {
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.documents(collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)

      copyCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      //#delete-documents
      val deleteDocuments = SolrSource
        .fromTupleStream(stream2)
        .map { tuple: Tuple =>
          val id = tuple.fields.get("title").toString
          WriteMessage.createDeleteMessage[SolrInputDocument](id)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.documents(collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)
      //#delete-documents

      deleteDocuments.futureValue

      val stream3 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream3)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq.empty[String]
    }

    "consume and update atomically documents" in {
      // Copy collection1 to collection2 through document stream
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      val upsertCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
            .copy(comment = "Written by good authors.")
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.documents(collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)

      upsertCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      //#update-atomically-documents
      val updateCollection = SolrSource
        .fromTupleStream(stream2)
        .map { tuple: Tuple =>
          val id = tuple.fields.get("title").toString
          val comment = tuple.fields.get("comment").toString
          WriteMessage.createUpdateMessage[SolrInputDocument](
            idField = "title",
            idValue = id,
            updates = Map(
              "comment" ->
              Map("set" -> (comment + " It is a good book!!!"))
            )
          )
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.documents(collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)
      //#update-atomically-documents

      updateCollection.futureValue

      val stream3 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream3)
        .map(tupleToBook)
        .map { b =>
          b.title + ". " + b.comment
        }
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq(
        "Akka Concurrency. Written by good authors. It is a good book!!!",
        "Akka in Action. Written by good authors. It is a good book!!!",
        "Effective Akka. Written by good authors. It is a good book!!!",
        "Learning Scala. Written by good authors. It is a good book!!!",
        "Programming in Scala. Written by good authors. It is a good book!!!",
        "Scala Puzzlers. Written by good authors. It is a good book!!!",
        "Scala for Spark in Production. Written by good authors. It is a good book!!!"
      )
    }

    "consume and delete beans" in {
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.typeds[Book](
            collectionName,
            SolrUpdateSettings(),
            binder = bookToDoc
          )
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)

      copyCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      val deleteElements = SolrSource
        .fromTupleStream(stream2)
        .map { tuple: Tuple =>
          val title = tuple.fields.get("title").toString
          WriteMessage.createDeleteMessage[Book](title)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.typeds[Book](
            collectionName,
            SolrUpdateSettings().withCommitWithin(5),
            binder = bookToDoc
          )
        )

      deleteElements.futureValue

      val stream3 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream3)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq.empty[String]
    }

    "consume and update atomically beans" in {
      val collectionName = createCollection(Some("router"))
      val stream = getTupleStream(predefinedCollection)

      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book =
            tupleToBook(tuple).copy(comment = "Written by good authors.", routerOpt = Some("router-value"))
          WriteMessage.createUpsertMessage(book)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.typeds[Book](
            collectionName,
            SolrUpdateSettings(),
            binder = bookToDoc
          )
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)

      copyCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      val updateCollection = SolrSource
        .fromTupleStream(stream2)
        .map { tuple: Tuple =>
          WriteMessage
            .createUpdateMessage[Book](
              idField = "title",
              tuple.fields.get("title").toString,
              updates = Map("comment" -> Map("set" -> (tuple.fields.get("comment") + " It is a good book!!!")))
            )
            .withRoutingFieldValue("router-value")
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.typeds[Book](
            collectionName,
            SolrUpdateSettings(),
            binder = bookToDoc
          )
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)

      updateCollection.futureValue

      val stream3 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream3)
        .map(tupleToBook)
        .map { b =>
          b.title + ". " + b.comment
        }
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq(
        "Akka Concurrency. Written by good authors. It is a good book!!!",
        "Akka in Action. Written by good authors. It is a good book!!!",
        "Effective Akka. Written by good authors. It is a good book!!!",
        "Learning Scala. Written by good authors. It is a good book!!!",
        "Programming in Scala. Written by good authors. It is a good book!!!",
        "Scala Puzzlers. Written by good authors. It is a good book!!!",
        "Scala for Spark in Production. Written by good authors. It is a good book!!!"
      )
    }

    "consume and delete documents by query" in {
      val collectionName = createCollection()
      val stream = getTupleStream(predefinedCollection)

      val copyCollection = SolrSource
        .fromTupleStream(stream)
        .map { tuple: Tuple =>
          val book: Book = tupleToBook(tuple)
          val doc: SolrInputDocument = bookToDoc(book)
          WriteMessage.createUpsertMessage(doc)
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.documents(collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)

      copyCollection.futureValue

      val stream2 = getTupleStream(collectionName)

      //#delete-documents-query
      val deleteByQuery = SolrSource
        .fromTupleStream(stream2)
        .map { tuple: Tuple =>
          val title = tuple.fields.get("title").toString
          WriteMessage.createDeleteByQueryMessage[SolrInputDocument](
            s"""title:"$title" """
          )
        }
        .groupedWithin(5, 10.millis)
        .runWith(
          SolrSink.documents(collectionName, SolrUpdateSettings())
        )
        // explicit commit when stream ended
        .map { _ =>
          solrClient.commit(collectionName)
        }(commitExecutionContext)
      //#delete-documents-query

      deleteByQuery.futureValue

      val stream3 = getTupleStream(collectionName)

      val res2 = SolrSource
        .fromTupleStream(stream3)
        .map(tupleToBook)
        .map(_.title)
        .runWith(Sink.seq)

      res2.futureValue shouldEqual Seq.empty[String]
    }

    "pass through only (Kafka example)" in {
      val collectionName = createCollection()

      var committedOffsets = List[CommittableOffset]()

      case class CommittableOffset(offset: Int) {
        def commitScaladsl(): Future[Done] = {
          committedOffsets = committedOffsets :+ this
          Future.successful(Done)
        }
      }

      case class CommittableOffsetBatch(offsets: immutable.Seq[CommittableOffset]) {
        def commitScaladsl(): Future[Done] = {
          committedOffsets = committedOffsets ++ offsets
          Future.successful(Done)
        }
      }

      case class CommittableMessage(book: Book, committableOffset: CommittableOffset)

      val messagesFromKafka = List(
        CommittableOffset(0),
        CommittableOffset(1),
        CommittableOffset(2)
      )
      val kafkaConsumerSource = Source(messagesFromKafka)
      //#kafka-example-PT
      // Note: This code mimics Alpakka Kafka APIs
      val copyCollection = kafkaConsumerSource
        .map { offset: CommittableOffset =>
          // Transform message so that we can write to solr
          WriteMessage.createPassThrough(offset).withSource(new SolrInputDocument())
        }
        .groupedWithin(5, 10.millis)
        .via( // write to Solr
          SolrFlow.documentsWithPassThrough[CommittableOffset](
            collectionName,
            // use implicit commits to Solr
            SolrUpdateSettings().withCommitWithin(5)
          )
        ) // check status and collect Kafka offsets
        .map { messageResults =>
          val offsets = messageResults.map { result =>
            if (result.status != 0)
              throw new Exception("Failed to write message to Solr")
            result.passThrough
          }
          CommittableOffsetBatch(offsets)
        }
        .mapAsync(1)(_.commitScaladsl())
        .runWith(Sink.ignore)
      //#kafka-example-PT

      copyCollection.futureValue

      // Make sure all messages was committed to kafka
      assert(List(0, 1, 2) == committedOffsets.map(_.offset))
    }

  }

  override def beforeAll(): Unit = {
    setupCluster()

    //#solr-update-settings
    import akka.stream.alpakka.solr.SolrUpdateSettings

    val settings = SolrUpdateSettings()
      .withCommitWithin(-1)
    //#solr-update-settings
    settings.toString shouldBe SolrUpdateSettings().toString

    CollectionAdminRequest
      .createCollection(predefinedCollection, "conf", 1, 1)
      .process(solrClient)

    new UpdateRequest()
      .add("title", "Akka in Action")
      .add("title", "Programming in Scala")
      .add("title", "Learning Scala")
      .add("title", "Scala for Spark in Production")
      .add("title", "Scala Puzzlers")
      .add("title", "Effective Akka")
      .add("title", "Akka Concurrency")
      .commit(solrClient, predefinedCollection)
  }

  override def afterAll(): Unit = {
    solrClient.close()
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
    solrClient.getClusterStateProvider
      .asInstanceOf[ZkClientClusterStateProvider]
      .uploadConfig(confDir.toPath, "conf")
    solrClient.setIdField("router")

    assertTrue(!solrClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
  }

  private val number = new AtomicInteger(2)

  private def createCollection(routerFieldOpt: Option[String] = None) = {
    val name = s"scala-collection-${number.incrementAndGet()}"
    CollectionAdminRequest
      .createCollection(name, "conf", 1, 1)
      .setRouterField(routerFieldOpt.orNull)
      .process(solrClient)
    name
  }

  private def getTupleStream(collection: String): TupleStream = {
    //#tuple-stream
    val factory = new StreamFactory().withCollectionZkHost(collection, zookeeperHost)
    val solrClientCache = new SolrClientCache()
    val streamContext = new StreamContext()
    streamContext.setSolrClientCache(solrClientCache)

    val expression =
      StreamExpressionParser.parse(s"""search($collection, q=*:*, fl="title,comment", sort="title asc")""")
    val stream: TupleStream = new CloudSolrStream(expression, factory)
    stream.setStreamContext(streamContext)

    val source = SolrSource
      .fromTupleStream(stream)
    //#tuple-stream
    source should not be null
    stream
  }

}
