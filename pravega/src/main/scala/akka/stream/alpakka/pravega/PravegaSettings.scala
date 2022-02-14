/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega
import java.net.URI
import java.time.Duration

import akka.actor.{ActorSystem, ClassicActorSystemProvider}
import com.typesafe.config.Config
import io.pravega.client.stream.{EventWriterConfig, ReaderConfig, Serializer}
import io.pravega.client.ClientConfig
import io.pravega.client.ClientConfig.ClientConfigBuilder
import io.pravega.client.stream.EventWriterConfig.EventWriterConfigBuilder
import io.pravega.client.stream.ReaderConfig.ReaderConfigBuilder
import io.pravega.client.tables.KeyValueTableClientConfiguration
import io.pravega.client.tables.TableKey

private[pravega] abstract class WithClientConfig(
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] = None
) {

  protected def handleClientConfig(): ClientConfig = {
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
                            timeout: Duration,
                            readerId: Option[String])
    extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def withClientConfig(clientConfig: ClientConfig): ReaderSettingsBuilder = copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ): ReaderSettingsBuilder =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def readerConfigBuilder(f: ReaderConfigBuilder => ReaderConfigBuilder): ReaderSettingsBuilder =
    copy(readerConfigCustomizer = Some(f))

  def withReaderId(id: String): ReaderSettingsBuilder = copy(readerId = Some(id))

  // JavaDSL
  def withTimeout(timeout: java.time.Duration): ReaderSettingsBuilder = copy(timeout = timeout)
  // ScalaDSL
  def withTimeout(timeout: scala.concurrent.duration.Duration): ReaderSettingsBuilder =
    copy(timeout = Duration.ofMillis(timeout.toMillis))

  def withSerializer[Message](serializer: Serializer[Message]): ReaderSettings[Message] = {

    readerConfigCustomizer.foreach(_(readerConfigBuilder))

    new ReaderSettings[Message](handleClientConfig(),
                                readerConfigBuilder.build(),
                                timeout.toMillis,
                                serializer,
                                readerId)
  }

  private def copy(clientConfig: Option[ClientConfig] = clientConfig,
                   clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] =
                     clientConfigCustomization,
                   readerConfigBuilder: ReaderConfigBuilder = readerConfigBuilder,
                   readerConfigCustomizer: Option[ReaderConfigBuilder => ReaderConfigBuilder] = readerConfigCustomizer,
                   timeout: Duration = timeout,
                   readerId: Option[String] = readerId) =
    new ReaderSettingsBuilder(config,
                              clientConfig,
                              clientConfigCustomization,
                              readerConfigBuilder,
                              readerConfigCustomizer,
                              timeout,
                              readerId)
}

/**
 * Reader settings that must be provided to @see [[akka.stream.alpakka.pravega.scaladsl.Pravega#source]]
 *
 * Built with @see [[ReaderSettingsBuilder]]
 *
 * @param clientConfig
 * @param readerConfig
 * @param timeout
 * @param serializer
 * @param readerId
 * @tparam Message
 */
class ReaderSettings[Message] private[pravega] (val clientConfig: ClientConfig,
                                                val readerConfig: ReaderConfig,
                                                val timeout: Long,
                                                val serializer: Serializer[Message],
                                                val readerId: Option[String])

object ReaderSettingsBuilder {
  val configPath = "akka.alpakka.pravega.reader"

  /**
   * Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[Message](actorSystem: ActorSystem): ReaderSettingsBuilder =
    apply(actorSystem.settings.config.getConfig(configPath))

  /**
   * Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[Message](actorSystem: ClassicActorSystemProvider): ReaderSettingsBuilder =
    apply(actorSystem.classicSystem)

  /**
   * Java API: Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[Message](actorSystem: ActorSystem): ReaderSettingsBuilder =
    apply(actorSystem)

  /**
   * Java API: Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[Message](actorSystem: ClassicActorSystemProvider): ReaderSettingsBuilder =
    apply(actorSystem.classicSystem)

  /**
   * Java API: Create settings from a configuration with the same layout as
   * * the default configuration `akka.alpakka.pravega.reader`.
   */
  def create[Message](config: Config): ReaderSettingsBuilder =
    apply(config)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.alpakka.pravega.reader`.
   */
  def apply[Message](implicit config: Config): ReaderSettingsBuilder = {

    import ConfigHelper._

    val readerBasicSetting = new ReaderBasicSetting()
    extractString("group-name")(readerBasicSetting.withGroupName)
    extractDuration("timeout")(readerBasicSetting.withTimeout)
    extractString("reader-id")(readerBasicSetting.withReaderId)

    val readerConfigBuilder = ConfigHelper.buildReaderConfig(config)

    new ReaderSettingsBuilder(config,
                              None,
                              None,
                              readerConfigBuilder,
                              None,
                              readerBasicSetting.timeout,
                              readerBasicSetting.readerId)

  }
}

class WriterSettingsBuilder[Message](
    config: Config,
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] = None,
    eventWriterConfigBuilder: EventWriterConfigBuilder,
    eventWriterConfigCustomizer: Option[EventWriterConfigBuilder => EventWriterConfigBuilder] = None,
    maximumInflightMessages: Int,
    keyExtractor: Option[Message => String]
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def eventWriterConfigBuilder(
      f: EventWriterConfigBuilder => EventWriterConfigBuilder
  ): WriterSettingsBuilder[Message] =
    copy(eventWriterConfigCustomizer = Some(f))

  def withClientConfig(clientConfig: ClientConfig) = copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ): WriterSettingsBuilder[Message] =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def withMaximumInflightMessages(i: Int): WriterSettingsBuilder[Message] = copy(maximumInflightMessages = i)

  def withKeyExtractor(keyExtractor: Message => String): WriterSettingsBuilder[Message] =
    copy(keyExtractor = Some(keyExtractor))

  private def copy(clientConfig: Option[ClientConfig] = clientConfig,
                   clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] =
                     clientConfigCustomization,
                   eventWriterConfigCustomizer: Option[EventWriterConfigBuilder => EventWriterConfigBuilder] =
                     eventWriterConfigCustomizer,
                   maximumInflightMessages: Int = maximumInflightMessages,
                   keyExtractor: Option[Message => String] = keyExtractor): WriterSettingsBuilder[Message] =
    new WriterSettingsBuilder(config,
                              clientConfig,
                              clientConfigCustomization,
                              eventWriterConfigBuilder,
                              eventWriterConfigCustomizer,
                              maximumInflightMessages,
                              keyExtractor)

  def withSerializer(serializer: Serializer[Message]): WriterSettings[Message] = {

    eventWriterConfigCustomizer.foreach(_(eventWriterConfigBuilder))

    val eventWriterConfig = eventWriterConfigBuilder.build()
    new WriterSettings[Message](handleClientConfig(),
                                eventWriterConfig,
                                serializer,
                                keyExtractor,
                                maximumInflightMessages)
  }

}

object WriterSettingsBuilder {
  val configPath = "akka.alpakka.pravega.writer"

  import ConfigHelper._

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[Message](actorSystem: ActorSystem): WriterSettingsBuilder[Message] =
    apply(actorSystem.settings.config.getConfig(configPath))

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[Message](actorSystem: ClassicActorSystemProvider): WriterSettingsBuilder[Message] =
    apply(actorSystem.classicSystem)

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[Message](actorSystem: ActorSystem): WriterSettingsBuilder[Message] =
    apply(actorSystem)

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[Message](actorSystem: ClassicActorSystemProvider): WriterSettingsBuilder[Message] =
    apply(actorSystem.classicSystem)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.alpakka.pravega`.
   */
  def apply[Message](config: Config): WriterSettingsBuilder[Message] =
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
    extractInt("initial-backoff-millis")(builder.initialBackoffMillis)
    extractInt("retry-attempts")(builder.retryAttempts)
    extractLong("transaction-timeout-time")(builder.transactionTimeoutTime)

    builder
  }

}

class TableReaderSettingsBuilder[K, V](
    config: Config,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    tableKeyExtractor: Option[K => TableKey],
    clientConfig: Option[ClientConfig] = None,
    clientConfigModifier: Option[ClientConfigBuilder => ClientConfigBuilder] = None,
    keyValueTableClientConfigurationBuilder: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder,
    keyValueTableClientConfigurationBuilderCustomizer: Option[
      KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
    ] = None,
    maximumInflightMessages: Int,
    maxEntriesAtOnce: Int
) extends WithClientConfig(config, clientConfig, clientConfigModifier) {
  def keyValueTableClientConfigurationBuilder(
      f: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
  ): TableReaderSettingsBuilder[K, V] =
    copy(keyValueTableClientConfigurationBuilderCustomizer = Some(f))

  def withClientConfigModifier(clientConfig: ClientConfig) = copy(clientConfig = Some(clientConfig))

  def withMaximumInflightMessages(i: Int): TableReaderSettingsBuilder[K, V] = copy(maximumInflightMessages = i)

  def withMaxEntriesAtOnce(i: Int): TableReaderSettingsBuilder[K, V] = copy(maxEntriesAtOnce = i)

  def clientConfigBuilder(
      clientConfigModifier: ClientConfigBuilder => ClientConfigBuilder
  ): TableReaderSettingsBuilder[K, V] =
    copy(clientConfigModifier = Some(clientConfigModifier))

  private def copy(
      clientConfig: Option[ClientConfig] = clientConfig,
      tableKeyExtractor: Option[K => TableKey] = tableKeyExtractor,
      clientConfigModifier: Option[ClientConfigBuilder => ClientConfigBuilder] = clientConfigModifier,
      keyValueTableClientConfigurationBuilderCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      maxEntriesAtOnce: Int = maxEntriesAtOnce
  ): TableReaderSettingsBuilder[K, V] =
    new TableReaderSettingsBuilder(config,
                                   keySerializer,
                                   valueSerializer,
                                   tableKeyExtractor,
                                   clientConfig,
                                   clientConfigModifier,
                                   keyValueTableClientConfigurationBuilder,
                                   keyValueTableClientConfigurationBuilderCustomizer,
                                   maximumInflightMessages,
                                   maxEntriesAtOnce)

  @deprecated("Use withKeyExtractor instead", "4.0.0")
  def withTableKey(extractor: K => TableKey): TableReaderSettingsBuilder[K, V] = withKeyExtractor(extractor)

  def withKeyExtractor(
      extractor: K => TableKey
  ): TableReaderSettingsBuilder[K, V] = copy(tableKeyExtractor = Some(extractor))

  def build(): TableReaderSettings[K, V] = {

    keyValueTableClientConfigurationBuilderCustomizer.foreach(_(keyValueTableClientConfigurationBuilder))

    val clientConfig = keyValueTableClientConfigurationBuilder.build()
    new TableReaderSettings[K, V](
      handleClientConfig(),
      clientConfig,
      keySerializer,
      valueSerializer,
      tableKeyExtractor.getOrElse(k => new TableKey(keySerializer.serialize(k))),
      maximumInflightMessages,
      maxEntriesAtOnce
    )
  }

}

object TableReaderSettingsBuilder {
  val configPath = "akka.alpakka.pravega.table"

  /**
   * Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[K, V]()(implicit actorSystem: ActorSystem,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): TableReaderSettingsBuilder[K, V] =
    apply(actorSystem.settings.config.getConfig(configPath), keySerializer, valueSerializer)

  /**
   * Create reader settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[K, V](actorSystem: ActorSystem,
                   keySerializer: Serializer[K],
                   valueSerializer: Serializer[V]): TableReaderSettingsBuilder[K, V] =
    apply()(actorSystem, keySerializer, valueSerializer)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.alpakka.pravega`.
   */
  def apply[K, V](config: Config,
                  keySerializer: Serializer[K],
                  valueSerializer: Serializer[V]): TableReaderSettingsBuilder[K, V] =
    new TableReaderSettingsBuilder(config,
                                   keySerializer,
                                   valueSerializer,
                                   None,
                                   None,
                                   None,
                                   tableClientConfiguration(config),
                                   None,
                                   config.getInt("maximum-inflight-messages"),
                                   config.getInt("max-entries-at-once"))

  private def tableClientConfiguration(implicit config: Config) = {

    import ConfigHelper._

    val builder = KeyValueTableClientConfiguration.builder()

    extractInt("backoff-multiple")(builder.backoffMultiple)
    extractInt("initial-backoff-millis")(builder.initialBackoffMillis)
    extractInt("max-backoff-millis")(builder.maxBackoffMillis)
    extractInt("retry-attempts")(builder.retryAttempts)

    builder
  }
}

class TableWriterSettingsBuilder[K, V](
    config: Config,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    tableKeyExtractor: Option[K => TableKey],
    clientConfig: Option[ClientConfig] = None,
    clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] = None,
    keyValueTableClientConfigurationBuilder: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder,
    keyValueTableClientConfigurationBuilderCustomizer: Option[
      KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
    ] = None,
    maximumInflightMessages: Int,
    maxEntriesAtOnce: Int
) extends WithClientConfig(config, clientConfig, clientConfigCustomization) {

  def keyValueTableClientConfigurationBuilder(
      f: KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
  ): TableWriterSettingsBuilder[K, V] =
    copy(keyValueTableClientConfigurationBuilderCustomizer = Some(f))

  def withClientConfig(clientConfig: ClientConfig) = copy(clientConfig = Some(clientConfig))

  def clientConfigBuilder(
      clientConfigCustomization: ClientConfigBuilder => ClientConfigBuilder
  ): TableWriterSettingsBuilder[K, V] =
    copy(clientConfigCustomization = Some(clientConfigCustomization))

  def withMaximumInflightMessages(i: Int): TableWriterSettingsBuilder[K, V] = copy(maximumInflightMessages = i)

  def withMaxEntriesAtOnce(i: Int): TableWriterSettingsBuilder[K, V] = copy(maxEntriesAtOnce = i)

  private def copy(
      tableKeyExtractor: Option[K => TableKey] = tableKeyExtractor,
      clientConfig: Option[ClientConfig] = clientConfig,
      clientConfigCustomization: Option[ClientConfigBuilder => ClientConfigBuilder] = clientConfigCustomization,
      keyValueTableClientConfigurationBuilderCustomizer: Option[
        KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder => KeyValueTableClientConfiguration.KeyValueTableClientConfigurationBuilder
      ] = keyValueTableClientConfigurationBuilderCustomizer,
      maximumInflightMessages: Int = maximumInflightMessages,
      maxEntriesAtOnce: Int = maxEntriesAtOnce
  ): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(config,
                                   keySerializer,
                                   valueSerializer,
                                   tableKeyExtractor,
                                   clientConfig,
                                   clientConfigCustomization,
                                   keyValueTableClientConfigurationBuilder,
                                   keyValueTableClientConfigurationBuilderCustomizer,
                                   maximumInflightMessages,
                                   maxEntriesAtOnce)

  def withKeyExtractor(
      extractor: K => TableKey
  ): TableWriterSettingsBuilder[K, V] = copy(tableKeyExtractor = Some(extractor))

  def build(): TableWriterSettings[K, V] = {

    keyValueTableClientConfigurationBuilderCustomizer.foreach(_(keyValueTableClientConfigurationBuilder))

    val eventWriterConfig = keyValueTableClientConfigurationBuilder.build()
    new TableWriterSettings[K, V](handleClientConfig(),
                                  eventWriterConfig,
                                  keySerializer,
                                  valueSerializer,
                                  tableKeyExtractor.getOrElse(k => new TableKey(keySerializer.serialize(k))),
                                  maximumInflightMessages)
  }

}

object TableWriterSettingsBuilder {
  val configPath = "akka.alpakka.pravega.table"

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def apply[K, V]()(implicit actorSystem: ActorSystem,
                    keySerializer: Serializer[K],
                    valueSerializer: Serializer[V]): TableWriterSettingsBuilder[K, V] =
    apply(actorSystem.settings.config.getConfig(configPath), keySerializer, valueSerializer)

  /**
   * Create writer settings from the default configuration
   * `akka.alpakka.pravega`.
   */
  def create[K, V](actorSystem: ActorSystem,
                   keySerializer: Serializer[K],
                   valueSerializer: Serializer[V]): TableWriterSettingsBuilder[K, V] =
    apply()(actorSystem, keySerializer, valueSerializer)

  /**
   * Create settings from a configuration with the same layout as
   * the default configuration `akka.alpakka.pravega`.
   */
  def apply[K, V](config: Config,
                  keySerializer: Serializer[K],
                  valueSerializer: Serializer[V]): TableWriterSettingsBuilder[K, V] =
    new TableWriterSettingsBuilder(config,
                                   keySerializer,
                                   valueSerializer,
                                   None,
                                   None,
                                   None,
                                   tableClientConfiguration(config),
                                   None,
                                   config.getInt("maximum-inflight-messages"),
                                   config.getInt("max-entries-at-once"))

  private def tableClientConfiguration(implicit config: Config) = {

    import ConfigHelper._

    val builder = KeyValueTableClientConfiguration.builder()

    extractInt("backoff-multiple")(builder.backoffMultiple)
    extractInt("initial-backoff-millis")(builder.initialBackoffMillis)
    extractInt("max-backoff-millis")(builder.maxBackoffMillis)
    extractInt("retry-attempts")(builder.retryAttempts)

    builder
  }
}

private[pravega] class ReaderBasicSetting(
    var groupName: Option[String] = None,
    var readerId: Option[String] = None,
    var timeout: Duration = Duration.ofSeconds(5)
) {
  def withGroupName(name: String) = groupName = Some(name)
  def withReaderId(name: String) = readerId = Some(name)
  def withTimeout(t: Duration) = timeout = t
}

/**
 * Writer settings that must be provided to @see Sink [[akka.stream.alpakka.pravega.scaladsl.Pravega#sink]]
 * or @see Flow [[akka.stream.alpakka.pravega.scaladsl.Pravega#flow]]
 *
 * Built with @see [[WriterSettingsBuilder]]
 *
 * @param clientConfig
 * @param eventWriterConfig
 * @param serializer
 * @param keyExtractor
 * @param maximumInflightMessages
 */
class WriterSettings[Message](val clientConfig: ClientConfig,
                              val eventWriterConfig: EventWriterConfig,
                              val serializer: Serializer[Message],
                              val keyExtractor: Option[Message => String],
                              val maximumInflightMessages: Int)

/**
 * Table Writer settings that must be provided to @see Sink [[akka.stream.alpakka.pravega.scaladsl.PravegaTable.sink]]
 * or @see Flow [[akka.stream.alpakka.pravega.scaladsl.PravegaTable.writeFlow]]
 *
 * Built with @see [[WriterSettingsBuilder]]
 *
 */
class TableWriterSettings[K, V](
    clientConfig: ClientConfig,
    keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    tableKey: K => TableKey,
    maximumInflightMessages: Int
) extends TableSettings(clientConfig,
                          keySerializer,
                          valueSerializer,
                          tableKey,
                          keyValueTableClientConfiguration,
                          maximumInflightMessages)

class TableReaderSettings[K, V](
    clientConfig: ClientConfig,
    keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V],
    tableKey: K => TableKey,
    maximumInflightMessages: Int,
    val maxEntriesAtOnce: Int
) extends TableSettings(clientConfig,
                          keySerializer,
                          valueSerializer,
                          tableKey,
                          keyValueTableClientConfiguration,
                          maximumInflightMessages)

protected abstract class TableSettings[K, V](
    val clientConfig: ClientConfig,
    val keySerializer: Serializer[K],
    val valueSerializer: Serializer[V],
    val tableKey: K => TableKey,
    val keyValueTableClientConfiguration: KeyValueTableClientConfiguration,
    val maximumInflightMessages: Int
)

private[pravega] object ConfigHelper {
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
  def extractDuration(path: String)(f: Duration => Unit)(implicit config: Config): Unit =
    if (config.hasPath(path))
      f(config.getDuration(path))
}
