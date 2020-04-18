/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.debezium.impl;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.Clock;
import io.debezium.util.VariableLatch;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public final class LazyEmbeddedEngine implements LazyDebeziumEngine<SourceRecord> {
  public static final Field ENGINE_NAME =
      Field.create("name")
          .withDescription("Unique name for this connector instance.")
          .withValidation(Field::isRequired);

  public static final Field CONNECTOR_CLASS =
      Field.create("connector.class")
          .withDescription("The Java class for the connector")
          .withValidation(Field::isRequired);

  public static final Field OFFSET_STORAGE =
      Field.create("offset.storage")
          .withDescription(
              "The Java class that implements the `OffsetBackingStore` "
                  + "interface, used to periodically store offsets so that, upon "
                  + "restart, the connector can resume where it last left off.")
          .withDefault(FileOffsetBackingStore.class.getName());

  public static final Field OFFSET_STORAGE_FILE_FILENAME =
      Field.create(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG)
          .withDescription(
              "The file where offsets are to be stored. Required when "
                  + "'offset.storage' is set to the "
                  + FileOffsetBackingStore.class.getName()
                  + " class.")
          .withDefault("");

  public static final Field OFFSET_STORAGE_KAFKA_TOPIC =
      Field.create(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG)
          .withDescription(
              "The name of the Kafka topic where offsets are to be stored. "
                  + "Required with other properties when 'offset.storage' is set to the "
                  + KafkaOffsetBackingStore.class.getName()
                  + " class.")
          .withDefault("");

  public static final Field OFFSET_STORAGE_KAFKA_PARTITIONS =
      Field.create(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG)
          .withType(ConfigDef.Type.INT)
          .withDescription(
              "The number of partitions used when creating the offset storage topic. "
                  + "Required with other properties when 'offset.storage' is set to the "
                  + KafkaOffsetBackingStore.class.getName()
                  + " class.");

  public static final Field OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR =
      Field.create(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG)
          .withType(ConfigDef.Type.SHORT)
          .withDescription(
              "Replication factor used when creating the offset storage topic. "
                  + "Required with other properties when 'offset.storage' is set to the "
                  + KafkaOffsetBackingStore.class.getName()
                  + " class.");

  public static final Field OFFSET_FLUSH_INTERVAL_MS =
      Field.create("offset.flush.interval.ms")
          .withDescription("Interval at which to try committing offsets. The default is 1 minute.")
          .withDefault(60000L)
          .withValidation(Field::isNonNegativeInteger);

  public static final Field OFFSET_COMMIT_TIMEOUT_MS =
      Field.create("offset.flush.timeout.ms")
          .withDescription(
              "Maximum number of milliseconds to wait for records to flush and partition offset data to be"
                  + " committed to offset storage before cancelling the process and restoring the offset "
                  + "data to be committed in a future attempt.")
          .withDefault(5000L)
          .withValidation(Field::isPositiveInteger);

  public static final Field OFFSET_COMMIT_POLICY =
      Field.create("offset.commit.policy")
          .withDescription(
              "The fully-qualified class name of the commit policy type. This class must implement the interface "
                  + OffsetCommitPolicy.class.getName()
                  + ". The default is a periodic commit policy based upon time intervals.")
          .withDefault(
              io.debezium.embedded.spi.OffsetCommitPolicy.PeriodicCommitOffsetPolicy.class
                  .getName())
          .withValidation(Field::isClassName);

  protected static final Field INTERNAL_KEY_CONVERTER_CLASS =
      Field.create("internal.key.converter")
          .withDescription(
              "The Converter class that should be used to serialize and deserialize key data for offsets.")
          .withDefault(JsonConverter.class.getName());

  protected static final Field INTERNAL_VALUE_CONVERTER_CLASS =
      Field.create("internal.value.converter")
          .withDescription(
              "The Converter class that should be used to serialize and deserialize value data for offsets.")
          .withDefault(JsonConverter.class.getName());

  /** The array of fields that are required by each connectors. */
  public static final Field.Set CONNECTOR_FIELDS = Field.setOf(ENGINE_NAME, CONNECTOR_CLASS);

  /** The array of all exposed fields. */
  protected static final Field.Set ALL_FIELDS =
      CONNECTOR_FIELDS.with(
          OFFSET_STORAGE,
          OFFSET_STORAGE_FILE_FILENAME,
          OFFSET_FLUSH_INTERVAL_MS,
          OFFSET_COMMIT_TIMEOUT_MS,
          INTERNAL_KEY_CONVERTER_CLASS,
          INTERNAL_VALUE_CONVERTER_CLASS);

  private static final Duration WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_DEFAULT =
      Duration.ofSeconds(2);
  private static final String WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_PROP =
      "debezium.embedded.shutdown.pause.before.interrupt.ms";

  public static final class BuilderImpl implements Builder<SourceRecord> {
    private Configuration config;
    private ClassLoader classLoader;
    private Clock clock;
    private OffsetCommitPolicy offsetCommitPolicy = null;

    public BuilderImpl using(Configuration config) {
      this.config = config;
      return this;
    }

    @Override
    public BuilderImpl notifying(Consumer<SourceRecord> consumer) {
      return this;
    }

    @Override
    public BuilderImpl notifying(DebeziumEngine.ChangeConsumer<SourceRecord> handler) {
      return this;
    }

    @Override
    public BuilderImpl using(Properties config) {
      this.config = Configuration.from(config);
      return this;
    }

    @Override
    public BuilderImpl using(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    public BuilderImpl using(Clock clock) {
      this.clock = clock;
      return this;
    }

    @Override
    public BuilderImpl using(DebeziumEngine.CompletionCallback completionCallback) {
      throw new UnsupportedOperationException(
          "cannot call using CompletionCallback in LazyEmbeddedEngine");
    }

    @Override
    public BuilderImpl using(DebeziumEngine.ConnectorCallback connectorCallback) {
      throw new UnsupportedOperationException(
          "cannot call using ConnectorCallback in LazyEmbeddedEngine");
    }

    @Override
    public BuilderImpl using(OffsetCommitPolicy offsetCommitPolicy) {
      this.offsetCommitPolicy = offsetCommitPolicy;
      return this;
    }

    @Override
    public BuilderImpl using(java.time.Clock clock) {
      return using(
          new Clock() {

            @Override
            public long currentTimeInMillis() {
              return clock.millis();
            }
          });
    }

    @Override
    public LazyEmbeddedEngine build() {
      if (classLoader == null) {
        classLoader = getClass().getClassLoader();
      }
      if (clock == null) {
        clock = Clock.system();
      }
      return new LazyEmbeddedEngine(config, classLoader, clock, offsetCommitPolicy);
    }
  }

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Configuration config;
  private final Clock clock;
  private final ClassLoader classLoader;
  private final AtomicReference<Thread> runningThread = new AtomicReference<>();
  private final VariableLatch latch = new VariableLatch(0);
  private long recordsSinceLastCommit = 0;
  private long timeOfLastCommitMillis = 0;
  private OffsetCommitPolicy offsetCommitPolicy;

  private DebeziumEngine.RecordCommitter<SourceRecord> committer;
  private OffsetStorageWriter offsetWriter;
  private OffsetStorageReader offsetReader;
  private Duration commitTimeout;
  private OffsetBackingStore offsetStore;
  private SourceConnector connector;

  private SourceTask task;

  private LazyEmbeddedEngine(
      Configuration config,
      ClassLoader classLoader,
      Clock clock,
      OffsetCommitPolicy offsetCommitPolicy) {
    this.config = config;
    this.classLoader = classLoader;
    this.clock = clock;
    this.offsetCommitPolicy = offsetCommitPolicy;

    assert this.config != null;
    assert this.classLoader != null;
    assert this.clock != null;
    Converter keyConverter =
        config.getInstance(INTERNAL_KEY_CONVERTER_CLASS, Converter.class, () -> this.classLoader);
    keyConverter.configure(
        config.subset(INTERNAL_KEY_CONVERTER_CLASS.name() + ".", true).asMap(), true);
    Converter valueConverter =
        config.getInstance(INTERNAL_VALUE_CONVERTER_CLASS, Converter.class, () -> this.classLoader);
    Configuration valueConverterConfig = config;
    if (valueConverter instanceof JsonConverter) {
      // Make sure that the JSON converter is configured to NOT enable schemas ...
      valueConverterConfig =
          config.edit().with(INTERNAL_VALUE_CONVERTER_CLASS + ".schemas.enable", false).build();
    }
    valueConverter.configure(
        valueConverterConfig.subset(INTERNAL_VALUE_CONVERTER_CLASS.name() + ".", true).asMap(),
        false);

    // Create the worker config, adding extra fields that are required for validation of a worker
    // config
    // but that are not used within the embedded engine (since the source records are never
    // serialized) ...
    Map<String, String> embeddedConfig = config.asMap(ALL_FIELDS);
    embeddedConfig.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    embeddedConfig.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    WorkerConfig workerConfig = new EmbeddedConfig(embeddedConfig);

    final String engineName = config.getString(ENGINE_NAME);
    final String connectorClassName = config.getString(CONNECTOR_CLASS);
    // Only one thread can be in this part of the method at a time ...
    latch.countUp();

    if (!config.validateAndRecord(CONNECTOR_FIELDS, logger::error)) {
      return;
    }

    // Instantiate the connector ...
    this.connector = null;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends SourceConnector> connectorClass =
          (Class<SourceConnector>) classLoader.loadClass(connectorClassName);
      this.connector = connectorClass.getDeclaredConstructor().newInstance();
    } catch (Throwable t) {
      throw new RuntimeException("unable to create SourceConnector", t);
    }

    // Instantiate the offset store ...
    final String offsetStoreClassName = config.getString(OFFSET_STORAGE);
    this.offsetStore = null;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends OffsetBackingStore> offsetStoreClass =
          (Class<OffsetBackingStore>) classLoader.loadClass(offsetStoreClassName);
      this.offsetStore = offsetStoreClass.getDeclaredConstructor().newInstance();
    } catch (Throwable t) {
      throw new RuntimeException("unable to create OffsetBackingStore", t);
    }

    // Initialize the offset store ...
    try {
      offsetStore.configure(workerConfig);
      offsetStore.start();
    } catch (Throwable t) {
      return;
    }

    // Set up the offset commit policy ...
    if (this.offsetCommitPolicy == null) {
      this.offsetCommitPolicy =
          config.getInstance(
              LazyEmbeddedEngine.OFFSET_COMMIT_POLICY, OffsetCommitPolicy.class, config);
    }

    // Do nothing ...
    ConnectorContext context =
        new ConnectorContext() {
          @Override
          public void requestTaskReconfiguration() {
            // Do nothing ...
          }

          @Override
          public void raiseError(Exception e) {}
        };
    connector.initialize(context);
    offsetWriter = new OffsetStorageWriter(offsetStore, engineName, keyConverter, valueConverter);
    offsetReader =
        new OffsetStorageReaderImpl(offsetStore, engineName, keyConverter, valueConverter);
    commitTimeout = Duration.ofMillis(config.getLong(OFFSET_COMMIT_TIMEOUT_MS));

    // Start the connector with the given properties and get the task configurations ...
    connector.start(config.asMap());
    List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
    Class<? extends Task> taskClass = connector.taskClass();
    this.task = null;
    try {
      this.task = (SourceTask) taskClass.getDeclaredConstructor().newInstance();
    } catch (IllegalAccessException
        | InstantiationException
        | NoSuchMethodException
        | InvocationTargetException t) {
      throw new RuntimeException("unable to create SourceTask", t);
    }
    try {
      // TODO Auto-generated method stub
      SourceTaskContext taskContext =
          new SourceTaskContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
              return offsetReader;
            }

            public Map<String, String> configs() {
              // TODO Auto-generated method stub
              return null;
            }
          };
      this.task.initialize(taskContext);
      this.task.start(taskConfigs.get(0));
    } catch (Throwable t) {
      // Mask the passwords ...
      config = Configuration.from(taskConfigs.get(0)).withMaskedPasswords();
      String msg =
          "Unable to initialize and start connector's task class '"
              + taskClass.getName()
              + "' with config: "
              + config;

      throw new RuntimeException(msg, t);
    }

    recordsSinceLastCommit = 0;

    timeOfLastCommitMillis = clock.currentTimeInMillis();
    committer = buildRecordCommitter(offsetWriter, task, commitTimeout);
  }

  @Override
  public List<SourceRecord> poll() {
    List<SourceRecord> changeRecords = null;

    try {
      logger.debug("Embedded engine is polling task for records on thread {}", runningThread.get());
      changeRecords = task.poll(); // blocks until there are values ...
      logger.debug("Embedded engine returned from polling task for records");
    } catch (InterruptedException e) {
      logger.debug(
          "Embedded engine interrupted on thread {} while polling the task for records",
          runningThread.get());
      if (this.runningThread.get() == Thread.currentThread()) {
        Thread.currentThread().interrupt();
      }
    } catch (RetriableException e) {
      logger.info("Retrieable exception thrown, connector will be restarted", e);
    }

    return changeRecords;
  }

  @Override
  public DebeziumEngine.RecordCommitter<SourceRecord> committer() {
    return committer;
  }

  @Override
  public void run() {}

  protected RecordCommitter<SourceRecord> buildRecordCommitter(
      OffsetStorageWriter offsetWriter, SourceTask task, Duration commitTimeout) {
    return new RecordCommitter<SourceRecord>() {

      @Override
      public synchronized void markProcessed(SourceRecord record) throws InterruptedException {
        task.commitRecord(record);
        recordsSinceLastCommit += 1;
        offsetWriter.offset(record.sourcePartition(), record.sourceOffset());
      }

      @Override
      public synchronized void markBatchFinished() {
        maybeFlush(offsetWriter, offsetCommitPolicy, commitTimeout, task);
      }
    };
  }

  protected void maybeFlush(
      OffsetStorageWriter offsetWriter,
      OffsetCommitPolicy policy,
      Duration commitTimeout,
      SourceTask task) {
    // Determine if we need to commit to offset storage ...
    long timeSinceLastCommitMillis = clock.currentTimeInMillis() - timeOfLastCommitMillis;
    if (policy.performCommit(
        recordsSinceLastCommit, Duration.ofMillis(timeSinceLastCommitMillis))) {
      commitOffsets(offsetWriter, commitTimeout, task);
    }
  }

  protected void commitOffsets(
      OffsetStorageWriter offsetWriter, Duration commitTimeout, SourceTask task) {
    long started = clock.currentTimeInMillis();
    long timeout = started + commitTimeout.toMillis();
    if (!offsetWriter.beginFlush()) {
      return;
    }
    Future<Void> flush = offsetWriter.doFlush(this::completedFlush);
    if (flush == null) {
      return; // no offsets to commit ...
    }

    // Wait until the offsets are flushed ...
    try {
      flush.get(Math.max(timeout - clock.currentTimeInMillis(), 0), TimeUnit.MILLISECONDS);
      // if we've gotten this far, the offsets have been committed so notify the task
      task.commit();
      recordsSinceLastCommit = 0;
      timeOfLastCommitMillis = clock.currentTimeInMillis();
    } catch (InterruptedException e) {
      logger.warn("Flush of {} offsets interrupted, cancelling", this);
      offsetWriter.cancelFlush();
    } catch (ExecutionException e) {
      logger.error("Flush of {} offsets threw an unexpected exception: ", this, e);
      offsetWriter.cancelFlush();
    } catch (TimeoutException e) {
      logger.error("Timed out waiting to flush {} offsets to storage", this);
      offsetWriter.cancelFlush();
    }
  }

  protected void completedFlush(Throwable error, Void result) {
    if (error != null) {
      logger.error("Failed to flush {} offsets to storage: ", this, error);
    } else {
      logger.trace("Finished flushing {} offsets to storage", this);
    }
  }

  public boolean stop() {
    logger.debug("Stopping the embedded engine");
    // Signal that the run() method should stop ...
    Thread thread = this.runningThread.getAndSet(null);

    // First stop the task ...
    logger.debug("Stopping the task and engine");
    task.stop();
    // Always commit offsets that were captured from the source records we actually processed ...
    commitOffsets(offsetWriter, commitTimeout, task);

    try {
      offsetStore.stop();
    } finally {
      connector.stop();
    }

    latch.countDown();
    runningThread.set(null);

    if (thread != null) {
      try {
        latch.await(
            Long.parseLong(
                System.getProperty(
                    WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_PROP,
                    Long.toString(WAIT_FOR_COMPLETION_BEFORE_INTERRUPT_DEFAULT.toMillis()))),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException ignored) {
      }
      logger.debug(
          "Interrupting the embedded engine's thread {} (already interrupted: {})",
          thread,
          thread.isInterrupted());
      // Interrupt the thread in case it is blocked while polling the task for records ...
      thread.interrupt();
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    stop();
  }

  @Override
  public String toString() {
    return "LazyEmbeddedEngine{id=" + config.getString(ENGINE_NAME) + '}';
  }

  protected static class EmbeddedConfig extends WorkerConfig {
    private static final ConfigDef CONFIG;

    static {
      ConfigDef config = baseConfigDef();
      Field.group(config, "file", OFFSET_STORAGE_FILE_FILENAME);
      Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_TOPIC);
      Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_PARTITIONS);
      Field.group(config, "kafka", OFFSET_STORAGE_KAFKA_REPLICATION_FACTOR);
      CONFIG = config;
    }

    protected EmbeddedConfig(Map<String, String> props) {
      super(CONFIG, props);
    }
  }
}
