/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.logging.logback

import akka.Done
import akka.actor.{ActorRef, ClassicActorSystemProvider}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.stream.alpakka.google.{GoogleAttributes, GoogleSettings}
import akka.stream.alpakka.googlecloud.logging.impl.{LogEntryQueue, Resource, WithInsertId}
import akka.stream.alpakka.googlecloud.logging.logback.CloudLoggingAppender._
import akka.stream.alpakka.googlecloud.logging.model.{LogEntry, LogSeverity, MonitoredResource, WriteEntriesRequest}
import akka.stream.alpakka.googlecloud.logging.scaladsl.CloudLogging
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorAttributes, CompletionStrategy, Materializer, OverflowStrategy}
import akka.util.Helpers
import akka.util.JavaDurationConverters._
import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.{ILoggingEvent, IThrowableProxy, StackTraceElementProxy}
import ch.qos.logback.core.UnsynchronizedAppenderBase
import ch.qos.logback.core.util.Loader
import spray.json.DefaultJsonProtocol._
import spray.json.{JsObject, JsString, JsValue}

import java.time
import java.time.Instant
import java.util.concurrent.CompletionStage
import scala.annotation.tailrec
import scala.beans.BeanProperty
import scala.collection.{immutable, mutable}
import scala.compat.java8.FutureConverters._
import scala.concurrent.{Await, Future}
import scala.util.Try

/**
 * [[https://logback.qos.ch/ Logback]] appender for Google Cloud Logging.
 */
final class CloudLoggingAppender extends UnsynchronizedAppenderBase[ILoggingEvent] {
  // Heavily based on com.google.cloud.logging.logback.LoggingAppender

  private val NotStarted = Future.failed(new IllegalStateException("Appender has not been started"))

  @volatile private var loggingSink: ActorRef = _
  @volatile private var loggingSinkComplete: Future[Done] = NotStarted

  /**
   * The maximum capacity of the queue used to buffer log entries.
   *
   * Defaults to 256 if not set.
   */
  @BeanProperty var queueSize = 256

  /**
   * Buffered log entries get immediately flushed for logs at or above this level.
   *
   * Defaults to [[Level.ERROR]] if not set.
   */
  @BeanProperty var flushLevel: Level = Level.ERROR

  /**
   * The maximum time a log entry is queued before it is attempted to be flushed.
   *
   * Defaults to 1 second if not set.
   */
  @BeanProperty var flushWithin: time.Duration = time.Duration.ofSeconds(1)

  /**
   * The maximum time to wait for the appender to flush after being stopped.
   *
   * Defaults to 1 second if not set.
   */
  @BeanProperty var stopWithin: time.Duration = time.Duration.ofSeconds(1)

  /**
   * The [[OverflowStrategy]] to use; only `dropHead` and `dropTail` supported.
   *
   * Defaults to `dropHead` if not set.
   */
  @BeanProperty var overflowStrategy = "dropHead"

  /**
   * Sets the log filename.
   *
   * Defaults to `java.log` if not set.
   */
  @BeanProperty var log = "java.log"

  /**
   * Sets the type of the monitored resource.
   *
   * Must be a [[https://cloud.google.com/logging/docs/api/v2/resource-list supported]] resource type. `gae_app`, `gce_instance` and `container` are auto-detected.
   *
   * Auto-detects by default with `global` as fallback.
   */
  @BeanProperty var resourceType = ""

  private val enhancerClassNames = new mutable.HashSet[String]()

  /**
   * Sets the configuration path from which to read [[GoogleSettings]].
   *
   * If not set, uses the default settings path.
   */
  @BeanProperty var googleSettings: String = GoogleSettings.ConfigPath

  /**
   * Sets the [[ActorSystem]] that will host this appender.
   */
  @BeanProperty var actorSystem: ClassicActorSystemProvider = _

  /**
   * Add extra labels using classes that implement [[LoggingEnhancer]].
   *
   * @param enhancerClassName The FQCN of the [[LoggingEnhancer]]
   */
  def addEnhancer(enhancerClassName: String): Unit = {
    enhancerClassNames.add(enhancerClassName)
  }

  private def getMonitoredResource(projectId: String)(implicit mat: Materializer): Future[MonitoredResource] = {
    import mat.executionContext
    for {
      resource <- if (resourceType.isEmpty)
        Resource.autoDetect
      else
        Future.successful(Resource(resourceType))
      labels <- resource.labels
    } yield MonitoredResource(resource.`type`, labels).withLabel("project_id", projectId)
  }

  private def getEnhancers[T](classNames: Set[String]) = classNames.toSeq.flatMap(getEnhancer[T])

  private def getEnhancer[T](enhancerClassName: String) =
    Try {
      val clz = Loader.loadClass(enhancerClassName.trim).asInstanceOf[Class[T]]
      clz.getDeclaredConstructor().newInstance()
    }.toOption

  override def start(): Unit = synchronized {
    if (!isStarted) {

      if (actorSystem == null)
        throw new IllegalStateException("An ActorSystem must be set before starting CloudLoggingAppender")

      implicit val system = actorSystem.classicSystem

      val loggersDispatcher = system.settings.LoggersDispatcher
      implicit val dispatcher = system.dispatchers.lookup(loggersDispatcher)
      implicit val settings = GoogleSettings(googleSettings)

      val enhancers =
        if (enhancerClassNames.nonEmpty)
          getEnhancers[LoggingEnhancer](enhancerClassNames.toSet)
        else
          DefaultLoggingEnhancers

      val overflowStrategy = Helpers.toRootLowerCase(this.overflowStrategy) match {
        case "drophead" => OverflowStrategy.dropHead
        case "droptail" => OverflowStrategy.dropTail
        case x => throw new IllegalArgumentException(s"Unrecognized or unsupported overflowStrategy: $x")
      }

      val (ref, complete) = Source
        .actorRef[ILoggingEvent]({ case Done => CompletionStrategy.draining }: PartialFunction[Any, CompletionStrategy],
                                 PartialFunction.empty,
                                 1,
                                 OverflowStrategy.fail)
        .map(logEntryFor(enhancers))
        .via(WithInsertId[JsValue])
        // Critical that the queue remains *outside* of the futureSink to guarantee that it never backpressures
        .via(LogEntryQueue[JsValue](queueSize, severityFor(flushLevel), flushWithin.asScala, overflowStrategy))
        .toMat {
          Sink
            .futureSink {
              getMonitoredResource(settings.projectId).map { monitoredResource =>
                val template = WriteEntriesRequest()
                  .withLogName(s"projects/${settings.projectId}/logs/${log}")
                  .withResource(monitoredResource)
                Flow[immutable.Seq[LogEntry[JsValue]]]
                  .map(template.withEntries[JsValue])
                  .toMat(CloudLogging.writeEntries)(Keep.right)
                  // TODO Necessary until attribute inheritance is fixed
                  .addAttributes(
                    ActorAttributes.dispatcher(loggersDispatcher) and GoogleAttributes.settingsPath(googleSettings)
                  )
              }
            }
            .mapMaterializedValue(_.flatten)
        }(Keep.both)
        .addAttributes(ActorAttributes.dispatcher(loggersDispatcher) and GoogleAttributes.settingsPath(googleSettings))
        .run()

      this.loggingSink = ref
      this.loggingSinkComplete = complete

      super.start()
    }
  }

  override def append(eventObject: ILoggingEvent): Unit = {
    eventObject.prepareForDeferredProcessing()
    loggingSink ! eventObject
  }

  /**
   * A [[Future]] for monitoring this appender.
   *
   * It completes with [[Done]] when the appender has stopped and all queued events have been flushed.
   * If the appender encounters an error at any time then it fails with the thrown exception.
   *
   * This method is only available after calling [[start]] and before calling [[stop]];
   * otherwise it provides a [[Future]] that fails immediately with an [[IllegalStateException]].
   *
   * @see [[hasFlushed]] for Java API
   */
  def flushed: Future[Done] = loggingSinkComplete

  /**
   * Java API: A [[CompletionStage]] for monitoring this appender.
   *
   * It completes with [[Done]] when the appender has stopped and all queued events have been flushed.
   * If the appender encounters an error at any time then the it fails with the thrown exception.
   *
   * This method is only available after calling [[start]] and before calling [[stop]];
   * otherwise it provides a [[CompletionStage]] that fails immediately with an [[IllegalStateException]].
   *
   * @see [[flushed]] for Scala API
   */
  def hasFlushed: CompletionStage[Done] = loggingSinkComplete.toJava

  override def stop(): Unit = synchronized {
    if (isStarted) {
      super.stop()
      loggingSink ! Done
      loggingSink = null
      Try(Await.result(loggingSinkComplete, stopWithin.asScala)) // Wait, but ignore exceptions
      loggingSinkComplete = NotStarted
    }
  }

  private def logEntryFor(loggingEnhancers: Seq[LoggingEnhancer])(e: ILoggingEvent): LogEntry[JsValue] = {
    val payload = new StringBuilder().append(e.getFormattedMessage).append('\n')
    writeStack(e.getThrowableProxy, "", payload)

    val level = e.getLevel
    val severity = severityFor(level)

    val jsonContent = new mutable.HashMap[String, JsString]
    jsonContent("message") = JsString(payload.toString.trim)
    if (severity == LogSeverity.Error)
      jsonContent("@type") = JsString(Type)

    val builder = LogEntry(JsObject(jsonContent.toMap))
      .withTimestamp(Instant.ofEpochMilli(e.getTimeStamp))
      .withSeverity(severity)
      .withLabel(LevelNameKey, level.toString)
      .withLabel(LevelValueKey, level.toInt.toString)
      .withLabel(LoggerNameKey, e.getLoggerName)

    loggingEnhancers.foldLeft(builder) { (builder, enhancer) =>
      enhancer.enhanceLogEntry(builder, e)
    }
  }

  @tailrec
  private def writeStack(throwProxy: IThrowableProxy, prefix: String, payload: StringBuilder): Unit = {
    if (throwProxy == null) return

    payload
      .append(prefix)
      .append(throwProxy.getClassName)
      .append(": ")
      .append(throwProxy.getMessage)
      .append('\n')
    var trace = throwProxy.getStackTraceElementProxyArray
    if (trace == null)
      trace = new Array[StackTraceElementProxy](0)

    val commonFrames = throwProxy.getCommonFrames
    val printFrames = trace.length - commonFrames
    for (i <- 0 until printFrames) {
      payload.append("    ").append(trace(i)).append('\n')
    }
    if (commonFrames != 0)
      payload.append("    ... ").append(commonFrames).append(" common frames elided\n")

    writeStack(throwProxy.getCause, "caused by: ", payload)
  }

  /**
   * Transforms Logback logging levels to Cloud severity.
   *
   * @param level Logback logging level
   * @return Cloud severity level
   */
  private def severityFor(level: Level) = level.toInt match {
    case 5000 => LogSeverity.Debug // TRACE
    case 10000 => LogSeverity.Debug // DEBUG
    case 20000 => LogSeverity.Info // INFO
    case 30000 => LogSeverity.Warning // WARNING
    case 40000 => LogSeverity.Error // ERROR
    case _ => LogSeverity.Default
  }
}

object CloudLoggingAppender {
  private val LevelNameKey = "levelName"
  private val LevelValueKey = "levelValue"
  private val LoggerNameKey = "loggerName"
  private val Type = "type.googleapis.com/google.devtools.clouderrorreporting.v1beta1.ReportedErrorEvent"
  private val DefaultLoggingEnhancers = Seq(new MDCEventEnhancer)
}
