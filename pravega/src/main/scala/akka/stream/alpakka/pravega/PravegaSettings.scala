/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega
import java.net.URI

import akka.actor.ActorSystem
import com.typesafe.config.Config
import io.pravega.client.stream.{EventWriterConfig, ReaderConfig, Serializer}
import io.pravega.client.ClientConfig
import io.pravega.client.ClientConfig.ClientConfigBuilder
import io.pravega.client.stream.EventWriterConfig.EventWriterConfigBuilder
import io.pravega.client.stream.ReaderConfig.ReaderConfigBuilder

abstract class WithClientConfig(config: Config,
                                clientConfig: Option[ClientConfig] = None,
                                clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] = None) {

  def handleClientConfig() = {
    val rawClientConfig =
      clientConfig.getOrElse(ConfigHelper.buildClientConfigFromTypeSafeConfig(config))

    clientConfigCustomization match {
      case Some(cust) => cust(rawClientConfig.toBuilder).build()
      case _ => rawClientConfig
    }

  }

}

class ReaderSettingsBuilder(config: Config,
                            clientConfig: Option[ClientConfig] = None,
                            clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] = None,
                            readerConfigBuilder: ReaderConfigBuilder,
                            readerConfigCustomizer: Option[ReaderConfigBuilder => ReaderConfigBuilder] = None,
                            groupName: Option[String],
                            timeout: Long,
                            readerId: Option[String])
    extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def withClientConfig(clientConfig: ClientConfig): ReaderSettingsBuilder = copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder) =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def readerConfigBuilder(f: ReaderConfigBuilder => ReaderConfigBuilder) = copy(readerConfigCustomizer = Some(f))

  def withGroupName(name: String): ReaderSettingsBuilder = copy(groupName = Some(name))
  def withReaderId(id: String): ReaderSettingsBuilder = copy(readerId = Some(id))
  def withTimeout(t: Long): ReaderSettingsBuilder = copy(timeout = t)

  def withSerializer[A](serializer: Serializer[A]): ReaderSettings[A] = {

    readerConfigCustomizer.foreach(_(readerConfigBuilder))

    new ReaderSettings[A](handleClientConfig(),
                          readerConfigBuilder.build(),
                          groupName.getOrElse(throw new IllegalStateException("group-name is mandatory")),
                          timeout,
                          serializer,
                          readerId)
  }

  private def copy(clientConfig: Option[ClientConfig] = clientConfig,
                   clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] =
                     clientConfigCustomization,
                   readerConfigBuilder: ReaderConfigBuilder = readerConfigBuilder,
                   readerConfigCustomizer: Option[ReaderConfigBuilder => ReaderConfigBuilder] = readerConfigCustomizer,
                   groupName: Option[String] = groupName,
                   timeout: Long = timeout,
                   readerId: Option[String] = readerId) =
    new ReaderSettingsBuilder(config,
                              clientConfig,
                              clientConfigCustomization,
                              readerConfigBuilder,
                              readerConfigCustomizer,
                              groupName,
                              timeout,
                              readerId)
}

class ReaderSettings[A](val clientConfig: ClientConfig,
                        val readerConfig: ReaderConfig,
                        val groupName: String,
                        val timeout: Long,
                        val serializer: Serializer[A],
                        val readerId: Option[String])

object ReaderSettingsBuilder {
  val configPath = "akka.alpakka.pravega.reader"

  /**
   * Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[A](actorSystem: ActorSystem): ReaderSettingsBuilder =
    apply(actorSystem.settings.config.getConfig(configPath))

  /**
   * Java API: Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[A](actorSystem: ActorSystem): ReaderSettingsBuilder =
    apply(actorSystem)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.alpakka.pravega.reader`.
   */
  def apply[A](implicit config: Config): ReaderSettingsBuilder = {

    import ConfigHelper._

    val readerBasicSetting = new ReaderBasicSetting()
    extractString("group-name")(readerBasicSetting.withGroupName)
    extractLong("timeout")(readerBasicSetting.withTimeout)
    extractString("reader-id")(readerBasicSetting.withReaderId)

    val readerConfigBuilder = ConfigHelper.buildReaderConfig(config)

    new ReaderSettingsBuilder(config,
                              None,
                              None,
                              readerConfigBuilder,
                              None,
                              readerBasicSetting.groupName,
                              readerBasicSetting.timeout,
                              readerBasicSetting.readerId)

  }
}

class WriterSettingsBuilder[A](
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] = None,
    eventWriterConfigBuilder: EventWriterConfigBuilder,
    eventWriterConfigCustomizer: Option[EventWriterConfigBuilder => EventWriterConfigBuilder] = None,
    maximumInflightMessages: Int,
    keyExtractor: Option[A => String]
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def eventWriterConfigBuilder(f: EventWriterConfigBuilder => EventWriterConfigBuilder): WriterSettingsBuilder[A] =
    copy(eventWriterConfigCustomizer = Some(f))

  def withClientConfig(clientConfig: ClientConfig) = copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ): WriterSettingsBuilder[A] =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def withMaximumInflightMessages(i: Int): WriterSettingsBuilder[A] = copy(maximumInflightMessages = i)

  def withKeyExtractor(keyExtractor: A => String): WriterSettingsBuilder[A] = copy(keyExtractor = Some(keyExtractor))

  private def copy(clientConfig: Option[ClientConfig] = clientConfig,
                   clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] =
                     clientConfigCustomization,
                   eventWriterConfigCustomizer: Option[EventWriterConfigBuilder => EventWriterConfigBuilder] =
                     eventWriterConfigCustomizer,
                   maximumInflightMessages: Int = maximumInflightMessages,
                   keyExtractor: Option[A => String] = keyExtractor): WriterSettingsBuilder[A] =
    new WriterSettingsBuilder(config,
                              clientConfig,
                              clientConfigCustomization,
                              eventWriterConfigBuilder,
                              eventWriterConfigCustomizer,
                              maximumInflightMessages,
                              keyExtractor)

  /**
    Build the settings.
   */
  def withSerializer(serializer: Serializer[A]): WriterSettings[A] = {

    eventWriterConfigCustomizer.foreach(_(eventWriterConfigBuilder))

    val eventWriterConfig = eventWriterConfigBuilder.build()
    new WriterSettings[A](handleClientConfig(), eventWriterConfig, serializer, None, maximumInflightMessages)
  }

}

object WriterSettingsBuilder {
  val configPath = "akka.alpakka.pravega.writer"

  import ConfigHelper._

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[A](actorSystem: ActorSystem): WriterSettingsBuilder[A] =
    apply(actorSystem.settings.config.getConfig(configPath))

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[A](actorSystem: ActorSystem): WriterSettingsBuilder[A] =
    apply(actorSystem)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.alpakka.pravega`.
   */
  def apply[A](config: Config): WriterSettingsBuilder[A] =
    new WriterSettingsBuilder(config,
                              None,
                              None,
                              eventWriterConfig(config),
                              None,
                              config.getInt("maximum-inflight-messages"),
                              None)

  private def eventWriterConfig(readerConfig: Config): EventWriterConfigBuilder = {
    val builder = EventWriterConfig.builder()

    implicit val config = readerConfig.getConfig("config")

    extractBoolean("automatically-note-time")(builder.automaticallyNoteTime)
    extractInt("backoff-multiple")(builder.backoffMultiple)
    extractBoolean("enable-connection-pooling")(builder.enableConnectionPooling)
    extractInt("initial-backoff-millis")(builder.initalBackoffMillis)
    extractInt("retry-attempts")(builder.retryAttempts)
    extractLong("transaction-timeout-time")(builder.transactionTimeoutTime)

    builder
  }

}

class ReaderBasicSetting(
    var groupName: Option[String] = None,
    var readerId: Option[String] = None,
    var timeout: Long = 0
) {
  def withGroupName(name: String) = groupName = Some(name)
  def withReaderId(name: String) = readerId = Some(name)
  def withTimeout(t: Long) = timeout = t
}

class WriterSettings[A](val clientConfig: ClientConfig,
                        val eventWriterConfig: EventWriterConfig,
                        val serializer: Serializer[A],
                        val keyExtractor: Option[A => String],
                        val maximumInflightMessages: Int)

object ConfigHelper {
  def buildReaderConfig(config: Config): ReaderConfigBuilder = {
    val builder = ReaderConfig
      .builder()

    implicit val c = config.getConfig("config")
    extractBoolean("disable-time-windows")(builder.disableTimeWindows)
    extractLong("initial-allocation-delay")(builder.initialAllocationDelay)

    builder
  }

  def buildClientConfigFromTypeSafeConfig(config: Config): ClientConfig = {
    val builder = ClientConfig.builder()

    implicit val c = config.getConfig("client-config")
    extractString("controller-uri") { uri =>
      builder.controllerURI(new URI(uri))
    }
    extractBoolean("enable-tls-to-controller")(builder.enableTlsToController)
    extractBoolean("enable-tls-to-segment-store")(builder.enableTlsToSegmentStore)
    extractInt("max-connections-per-segment-store")(builder.maxConnectionsPerSegmentStore)
    extractString("trust-store")(builder.trustStore)
    extractBoolean("validate-host-name")(builder.validateHostName)

    builder.build()
  }

  def extractString(path: String)(f: String => Unit)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getString(path))
  def extractBoolean(path: String)(f: Boolean => Unit)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getBoolean(path))
  def extractInt(path: String)(f: Int => Unit)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getInt(path))
  def extractLong(path: String)(f: Long => Unit)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getLong(path))
}
