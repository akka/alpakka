/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.pravega;

import akka.Done;
import akka.NotUsed;
import akka.japi.Pair;

import akka.stream.alpakka.pravega.javadsl.PravegaTable;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import docs.javadsl.PravegaBaseTestCase;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.UTF8StringSerializer;

import io.pravega.client.tables.KeyValueTableConfiguration;
import io.pravega.client.tables.TableKey;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.ArrayList;
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
      TableWriterSettingsBuilder.<Integer, String>create(system, intSerializer, serializer)
          .withKeyExtractor(id -> new TableKey(intSerializer.serialize(id)))
          .build();

  @Test
  public void writeAndReadInKVTable()
      throws ExecutionException, InterruptedException, TimeoutException {

    final List<Pair<Integer, String>> events =
        Arrays.asList(
            new Pair<Integer, String>(1, "One"),
            new Pair<Integer, String>(2, "Two"),
            new Pair<Integer, String>(3, "Three"),
            new Pair<Integer, String>(4, "Four"));

    Sink<Pair<Integer, String>, CompletionStage<Done>> sink =
        PravegaTable.sink(scope, tableName, tablewriterSettings);

    CompletionStage<Done> done = Source.from(events).toMat(sink, Keep.right()).run(system);

    done.toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);

    TableReaderSettings<Integer, String> tableReaderSettings =
        TableReaderSettingsBuilder.<Integer, String>create(system, intSerializer, serializer)
            .withKeyExtractor(id -> new TableKey(intSerializer.serialize(id)))
            .build();

    final CompletionStage<String> readingDone =
        PravegaTable.source(scope, tableName, tableReaderSettings)
            .runWith(
                Sink.fold(
                    "",
                    (acc, p) -> {
                      if (acc == "") return p.value();
                      return acc + ", " + p.value();
                    }),
                system);

    String result = readingDone.toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);
    Assert.assertTrue(
        String.format("Read 2 elements [%s]", result), result.equals("One, Two, Three, Four"));

    Flow<Integer, Optional<String>, NotUsed> readFlow =
        PravegaTable.readFlow(scope, tableName, tableReaderSettings);

    List<Integer> ids = Arrays.asList(1, 2, 3, 4);

    CompletionStage<List<String>> readFlowFut =
        Source.from(ids)
            .via(readFlow)
            .runWith(
                Sink.fold(
                    new ArrayList<String>(),
                    (acc, p) -> {
                      acc.add(p.get());
                      return acc;
                    }),
                system);

    List<String> values = readFlowFut.toCompletableFuture().get(timeoutSeconds, TimeUnit.SECONDS);

    Assert.assertEquals(values, Arrays.asList("One", "Two", "Three", "Four"));
  }

  @BeforeClass
  public static void setUp() {
    createScope(scope);

    ClientConfig clientConfig = ClientConfig.builder().build();

    KeyValueTableConfiguration keyValueTableConfig =
        KeyValueTableConfiguration.builder().partitionCount(2).primaryKeyLength(4).build();
    KeyValueTableManager keyValueTableManager = KeyValueTableManager.create(clientConfig);

    if (keyValueTableManager.createKeyValueTable(scope, tableName, keyValueTableConfig))
      LOGGER.info("Created KeyValue table [{}] in scope [{}]", tableName, scope);
    else LOGGER.info("KeyValue table [{}] already exists in scope [{}]", tableName, scope);

    keyValueTableManager.close();
  }
}
