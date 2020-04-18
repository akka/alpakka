/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.scaladsl

import java.util
import java.util.concurrent.{ExecutorService, Executors}

import akka.NotUsed
import akka.actor.{ActorSystem, Props}
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.debezium.impl.{CommitterActor, LazyEmbeddedEngine}
import akka.stream.alpakka.debezium.{CommittableRecord, DebeziumSettings, SimpleRecord}
import akka.stream.scaladsl._
import akka.stream.{OverflowStrategy, QueueOfferResult}
import io.debezium.embedded.Connect
import io.debezium.engine.DebeziumEngine
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object DebeziumSource {
  def custom(settings: DebeziumSettings)(implicit s: ActorSystem): Source[CommittableRecord, NotUsed] = {
    val configurations = settings.properties

    val valueConverter = settings.internalValueConverter.getDeclaredConstructor().newInstance()
    valueConverter.configure(configurations.asInstanceOf[java.util.Map[String, String]], false)

    val engine = new LazyEmbeddedEngine.BuilderImpl().using(configurations).build()

    lazy val committer = engine.committer()

    val singleThreadEc = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor())

    Source
      .repeat(engine)
      .mapAsync(1) { engine =>
        Future {
          val records = engine.poll()
          if (records == null) List.empty else records.asScala.toList
        }(singleThreadEc)
      }
      .mapConcat { list =>
        val p = Promise[Unit]()
        val committerActor = s.actorOf(Props(new CommitterActor(list, p, committer)))

        list.map { record =>
          new CommittableRecord(record, committerActor, valueConverter)
        }
      }
      .alsoTo(Sink.onComplete { _ =>
        singleThreadEc.shutdownNow()
        engine.stop()
      })
  }

  def committable(settings: DebeziumSettings, executor: ExecutorService = Executors.newSingleThreadExecutor())(
      implicit s: ActorSystem
  ): Source[CommittableRecord, NotUsed] = {
    val (queue, source) =
      Source
        .queue[List[CommittableRecord]](settings.maxInMemoryElements, OverflowStrategy.backpressure)
        .mapConcat(identity)
        .preMaterialize()

    val configurations = settings.properties

    val valueConverter = settings.internalValueConverter.getDeclaredConstructor().newInstance()
    valueConverter.configure(configurations.asInstanceOf[java.util.Map[String, String]], false)

    val engine = DebeziumEngine
      .create[SourceRecord](classOf[Connect])
      .using(configurations)
      .notifying((records: util.List[SourceRecord], committer: DebeziumEngine.RecordCommitter[SourceRecord]) => {
        val list = records.asScala.toList
        val p = Promise[Unit]()
        val committerActor = s.actorOf(Props(new CommitterActor(list, p, committer)))

        def result: Future[Unit] =
          queue
            .offer(list.map(r => new CommittableRecord(r, committerActor, valueConverter)))
            .flatMap {
              case QueueOfferResult.Enqueued =>
                p.future
              case _ =>
                Future.failed(new InterruptedException("error while enqueueing records"))
            }(ExecutionContexts.sameThreadExecutionContext)

        Await.result(result, settings.acknowledgmentTimeout)
      })
      .using(
        (success: Boolean, _: String, error: Throwable) =>
          if (!success) queue.fail(error)
          else queue.complete()
      )
      .build()

    executor.execute(engine)
    source.mapMaterializedValue(_ => NotUsed)
  }

  def apply(settings: DebeziumSettings, executor: ExecutorService = Executors.newSingleThreadExecutor())(
      implicit s: ActorSystem
  ): Source[SimpleRecord, NotUsed] = {
    val (queue, source) =
      Source
        .queue[List[SimpleRecord]](settings.maxInMemoryElements, OverflowStrategy.backpressure)
        .mapConcat(identity)
        .preMaterialize()

    val configurations = settings.properties

    val valueConverter = settings.internalValueConverter.getDeclaredConstructor().newInstance()
    valueConverter.configure(configurations.asInstanceOf[java.util.Map[String, String]], false)

    val engine = DebeziumEngine
      .create[SourceRecord](classOf[Connect])
      .using(configurations)
      .notifying((records: util.List[SourceRecord], committer: DebeziumEngine.RecordCommitter[SourceRecord]) => {
        queue
          .offer(records.asScala.toList.map(r => new SimpleRecord(r, valueConverter)))
          .map {
            case QueueOfferResult.Enqueued =>
              records.forEach(e => committer.markProcessed(e))
              committer.markBatchFinished()
            case _ =>
              throw new InterruptedException("error while enqueueing records")
          }(ExecutionContexts.sameThreadExecutionContext)
      })
      .using(
        (success: Boolean, message: String, error: Throwable) =>
          if (!success) queue.fail(error)
          else queue.complete()
      )
      .build()

    executor.execute(engine)
    source.mapMaterializedValue(_ => NotUsed)
  }
}
