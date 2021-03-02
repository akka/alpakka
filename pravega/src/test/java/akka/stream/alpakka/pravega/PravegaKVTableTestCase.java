/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.pravega;

import akka.Done;
import akka.japi.Pair;

import akka.stream.alpakka.pravega.javadsl.Pravega;
import akka.stream.alpakka.pravega.javadsl.PravegaTable;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import docs.javadsl.PravegaBaseTestCase;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.client.tables.KeyValueTableClientConfiguration;
import io.pravega.client.tables.KeyValueTableConfiguration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

public class PravegaKVTableTestCase extends PravegaBaseTestCase {

  private long timeoutSeconds = 10;

  static String scope = newScope();
  static String tableName = newTableName();

  UTF8StringSerializer serializer = new UTF8StringSerializer();

  Serializer<Integer> intSerializer =
      new Serializer<Integer>() {
        public ByteBuffer serialize(Integer value) {
          ByteBuffer buff = ByteBuffer.allocate(4).putInt(value);
          buff.position(0);
          return buff;
        }

        public Integer deserialize(ByteBuffer serializedValue) {

          return serializedValue.getInt();
        }
      };

  TableWriterSettings<Integer, String> tablewriterSettings =
      TableWriterSettingsBuilder.<Integer, String>create(system)
          .withSerializers(intSerializer, serializer);

  @Test
  public void writeAndReadInKVTable()
      throws ExecutionException, InterruptedException, TimeoutException {

    final List<Person> events =
        Arrays.asList(
            new Person(1, "One"),
            new Person(2, "Two"),
            new Person(3, "Three"),
            new Person(4, "Four"));

    Sink<Person, CompletionStage<Done>> sink =
        PravegaTable.sink(
            scope,
            tableName,
            tablewriterSettings,
            (Person p) -> new Pair<>(p.id(), p.firstname()),
            (Person p) -> (p.id() % 2 == 0) ? "test" : null);

    CompletionStage<Done> done = Source.from(events).toMat(sink, Keep.right()).run(system);

    done.toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);

    TableSettings<Integer, String> tableSettings =
        TableSettingsBuilder.<Integer, String>create(system)
            .withKVSerializers(intSerializer, serializer);

    final CompletionStage<String> readingDone =
        PravegaTable.source(scope, tableName, "test", tableSettings)
            .runWith(
                Sink.fold(
                    "",
                    (acc, p) -> {
                      if (acc == "") return p.second();
                      return acc + ", " + p.second();
                    }),
                system);

    String result = readingDone.toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);
    Assert.assertTrue(String.format("Read 2 elements [%s]", result), result.equals("Two, Four"));
  }

  @BeforeClass
  public static void setUp() {
    createScope(scope);

    ClientConfig clientConfig = ClientConfig.builder().build();

    KeyValueTableConfiguration keyValueTableConfig =
        KeyValueTableConfiguration.builder().partitionCount(2).build();
    KeyValueTableManager keyValueTableManager = KeyValueTableManager.create(clientConfig);

    if (keyValueTableManager.createKeyValueTable(scope, tableName, keyValueTableConfig))
      LOGGER.info("Created KeyValue table [{}] in scope [{}]", tableName, scope);
    else LOGGER.info("KeyValue table [{}] already exists in scope [{}]", tableName, scope);

    keyValueTableManager.close();
  }
}
