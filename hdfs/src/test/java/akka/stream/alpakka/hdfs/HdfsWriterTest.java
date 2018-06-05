/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.hdfs;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.hdfs.javadsl.HdfsFlow;
import akka.stream.alpakka.hdfs.util.JavaTestUtils;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HdfsWriterTest {
  private static MiniDFSCluster hdfsCluster = null;
  private static ActorSystem system;
  private static ActorMaterializer materializer;
  private static String destionation = JavaTestUtils.destination();
  private static List<ByteString> books = JavaTestUtils.books();
  private static FileSystem fs = null;
  private static HdfsWritingSettings settings = HdfsWritingSettings.create();

  // #define-kafka-classes
  public static class Book {
    final String title;

    Book(String title) {
      this.title = title;
    }
  }

  static class KafkaCommitter {
    List<Integer> committedOffsets = new ArrayList<>();

    void commit(KafkaOffset offset) {
      committedOffsets.add(offset.offset);
    }
  }

  static class KafkaOffset {
    final int offset;

    KafkaOffset(int offset) {
      this.offset = offset;
    }
  }

  static class KafkaMessage {
    final Book book;
    final KafkaOffset offset;

    KafkaMessage(Book book, KafkaOffset offset) {
      this.book = book;
      this.offset = offset;
    }
  }
  // #define-kafka-classes

  @Test
  public void testDataWriterFileSizeRotationWithFiveFile() throws Exception {
    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(
            fs,
            SyncStrategyFactory.count(50),
            RotationStrategyFactory.size(0.01, FileUnit.KB()),
            settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(IncomingMessage::create).via(flow).runWith(Sink.seq(), materializer);

    List<RotationMessage> result = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Arrays.asList(
            new RotationMessage("0", 0),
            new RotationMessage("1", 1),
            new RotationMessage("2", 2),
            new RotationMessage("3", 3),
            new RotationMessage("4", 4));

    assertEquals(expect, result);
  }

  @Test
  public void testDataWriterFileSizeRotationWithTwoFile() throws Exception {
    List<ByteString> data = JavaTestUtils.generateFakeContent(1.0, FileUnit.KB().byteCount());

    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(
            fs,
            SyncStrategyFactory.count(500),
            RotationStrategyFactory.size(0.5, FileUnit.KB()),
            settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(data).map(IncomingMessage::create).via(flow).runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    assertEquals(logs.size(), 2);

    List<FileStatus> files = JavaTestUtils.getFiles(fs);
    assertEquals(files.stream().map(FileStatus::getLen).reduce((a, b) -> a + b).get().longValue(), 1024L);
    files.forEach(file -> assertTrue(file.getLen() <= 512L));
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyFlattenContent(fs, logs, data);
  }

  @Test
  public void testDataWriterDetectUpstreamFinish() throws Exception {
    List<ByteString> data = JavaTestUtils.generateFakeContent(1.0, FileUnit.KB().byteCount());

    // #define-data
    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(
            fs,
            SyncStrategyFactory.count(500),
            RotationStrategyFactory.size(1, FileUnit.GB()),
            settings);
    // #define-data
    CompletionStage<List<RotationMessage>> resF =
        Source.from(data).map(IncomingMessage::create).via(flow).runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    assertEquals(logs.size(), 1);

    assertEquals(JavaTestUtils.getFiles(fs).get(0).getLen(), 1024L);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyFlattenContent(fs, logs, data);
  }

  @Test
  public void testDataWriterWithBufferRotation() throws Exception {
    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(fs, SyncStrategyFactory.count(1), RotationStrategyFactory.count(2), settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(IncomingMessage::create).via(flow).runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    assertEquals(logs.size(), 3);
    JavaTestUtils.verifyFlattenContent(fs, logs, books);
  }

  @Test
  public void testDataWriterWithTimeRotation() throws Exception {
    Pair<Cancellable, CompletionStage<List<RotationMessage>>> resF =
        Source.tick(
                java.time.Duration.ofMillis(0),
                java.time.Duration.ofMillis(50),
                ByteString.fromString("I love Alpakka!"))
            .map(IncomingMessage::create)
            .via(
                HdfsFlow.data(
                    fs,
                    SyncStrategyFactory.none(),
                    RotationStrategyFactory.time(Duration.create(500, TimeUnit.MILLISECONDS)),
                    settings))
            .toMat(Sink.seq(), Keep.both())
            .run(materializer);

    system
        .scheduler()
        .scheduleOnce(
            java.time.Duration.ofMillis(1500),
            () -> resF.first().cancel(),
            system.dispatcher()); // cancel within 1500 milliseconds

    List<RotationMessage> logs = new ArrayList<>(resF.second().toCompletableFuture().get());
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    assertTrue(ArrayUtils.contains(new int[] {3, 4}, logs.size()));
  }

  @Test
  public void testDataWriterWithNoRotation() throws Exception {
    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(fs, SyncStrategyFactory.none(), RotationStrategyFactory.none(), settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(IncomingMessage::create).via(flow).runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    JavaTestUtils.verifyOutputFileSize(fs, logs);
    List<Integer> list = new ArrayList<>();
    books.iterator().forEachRemaining(s -> list.add(s.toArray().length));
    assertEquals(
        JavaTestUtils.getFiles(fs).get(0).getLen(),
        list.stream().mapToInt(Integer::intValue).sum());
  }

  @Test
  public void testDataWriterKafkaExample() throws Exception {
    // #kafka-example
    // We're going to pretend we got messages from kafka.
    // After we've written them to HDFS, we want
    // to commit the offset to Kafka
    List<KafkaMessage> messagesFromKafka =
        Arrays.asList(
            new KafkaMessage(new Book("Akka Concurrency"), new KafkaOffset(0)),
            new KafkaMessage(new Book("Akka in Action"), new KafkaOffset(1)),
            new KafkaMessage(new Book("Effective Akka"), new KafkaOffset(2)),
            new KafkaMessage(new Book("Learning Scala"), new KafkaOffset(3)),
            new KafkaMessage(new Book("Scala Puzzlers"), new KafkaOffset(4)),
            new KafkaMessage(new Book("Scala for Spark in Production"), new KafkaOffset(5)));

    final KafkaCommitter kafkaCommitter = new KafkaCommitter();

    Flow<IncomingMessage<ByteString, KafkaOffset>, OutgoingMessage<KafkaOffset>, NotUsed> flow =
        HdfsFlow.dataWithPassThrough(
            fs,
            SyncStrategyFactory.count(50),
            RotationStrategyFactory.count(4),
            HdfsWritingSettings.create().withNewLine(true));

    CompletionStage<List<RotationMessage>> resF =
        Source.from(messagesFromKafka)
            .map(
                kafkaMessage -> {
                  Book book = kafkaMessage.book;
                  // Transform message so that we can write to hdfs\
                  return IncomingMessage.create(
                      ByteString.fromString(book.title), kafkaMessage.offset);
                })
            .via(flow)
            .map(
                message -> {
                  if (message instanceof WrittenMessage) {
                    kafkaCommitter.commit(((WrittenMessage<KafkaOffset>) message).passThrough());
                    return message;
                  } else {
                    return message;
                  }
                })
            .collectType(RotationMessage.class) // Collect only rotation messages
            .runWith(Sink.seq(), materializer);
    // #kafka-example

    ArrayList<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect = Arrays.asList(new RotationMessage("0", 0), new RotationMessage("1", 1));

    // Make sure all messages was committed to kafka
    assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), kafkaCommitter.committedOffsets);

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    assertEquals(
        JavaTestUtils.readLogs(fs, logs)
            .stream()
            .map(string -> string.split("\n"))
            .flatMap(Arrays::stream)
            .collect(Collectors.toList()),
        messagesFromKafka.stream().map(message -> message.book.title).collect(Collectors.toList()));
  }

  @Test
  public void testCompressedDataWriterWithSizeRotation() throws Exception {
    // #define-codec
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());
    // #define-codec

    // #define-compress
    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.compressed(
            fs,
            SyncStrategyFactory.count(50),
            RotationStrategyFactory.size(0.1, FileUnit.MB()),
            codec,
            settings);
    // #define-compress

    List<ByteString> content =
        JavaTestUtils.generateFakeContentWithPartitions(1, FileUnit.MB().byteCount(), 30);

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(IncomingMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Arrays.asList(
            new RotationMessage("0.deflate", 0),
            new RotationMessage("1.deflate", 1),
            new RotationMessage("2.deflate", 2),
            new RotationMessage("3.deflate", 3),
            new RotationMessage("4.deflate", 4),
            new RotationMessage("5.deflate", 5));

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyLogsWithCodec(fs, content, logs, codec);
  }

  @Test
  public void testCompressedDataWriterWithBufferRotation() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());

    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.compressed(
            fs, SyncStrategyFactory.count(1), RotationStrategyFactory.count(1), codec, settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(IncomingMessage::create).via(flow).runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Arrays.asList(
            new RotationMessage("0.deflate", 0),
            new RotationMessage("1.deflate", 1),
            new RotationMessage("2.deflate", 2),
            new RotationMessage("3.deflate", 3),
            new RotationMessage("4.deflate", 4));

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyLogsWithCodec(fs, books, logs, codec);
  }

  @Test
  public void testCompressedDataWriterWithNoRotation() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());

    Flow<IncomingMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.compressed(
            fs, SyncStrategyFactory.none(), RotationStrategyFactory.none(), codec, settings);

    List<ByteString> content =
        JavaTestUtils.generateFakeContentWithPartitions(1, FileUnit.MB().byteCount(), 30);

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(IncomingMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect = Collections.singletonList(new RotationMessage("0.deflate", 0));

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyLogsWithCodec(fs, content, logs, codec);
  }

  @Test
  public void testSequenceWriterWithSizeRotationWithoutCompression() throws Exception {
    // #define-sequence-compressed
    Flow<IncomingMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs,
            SyncStrategyFactory.none(),
            RotationStrategyFactory.size(1, FileUnit.MB()),
            settings,
            Text.class,
            Text.class);
    // #define-sequence-compressed

    List<Pair<Text, Text>> content =
        JavaTestUtils.generateFakeContentForSequence(0.5, FileUnit.MB().byteCount());

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(IncomingMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @Test
  public void testSequenceWriterWithSizeRotationWithCompression() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());

    // #define-sequence
    Flow<IncomingMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs,
            SyncStrategyFactory.none(),
            RotationStrategyFactory.size(1, FileUnit.MB()),
            SequenceFile.CompressionType.BLOCK,
            codec,
            settings,
            Text.class,
            Text.class);
    // #define-sequence

    List<Pair<Text, Text>> content =
        JavaTestUtils.generateFakeContentForSequence(0.5, FileUnit.MB().byteCount());

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(IncomingMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @Test
  public void testSequenceWriterWithBufferRotation() throws Exception {
    Flow<IncomingMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs,
            SyncStrategyFactory.none(),
            RotationStrategyFactory.count(1),
            settings,
            Text.class,
            Text.class);

    List<Pair<Text, Text>> content = JavaTestUtils.booksForSequenceWriter();

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(IncomingMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    assertEquals(logs.size(), 5);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @Test
  public void testSequenceWriterWithNoRotation() throws Exception {
    Flow<IncomingMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs,
            SyncStrategyFactory.none(),
            RotationStrategyFactory.none(),
            settings,
            Text.class,
            Text.class);

    List<Pair<Text, Text>> content =
        JavaTestUtils.generateFakeContentForSequence(0.5, FileUnit.MB().byteCount());

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(IncomingMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    assertEquals(logs.size(), 1);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @BeforeClass
  public static void setup() throws Exception {
    setupCluster();

    // #init-client
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://localhost:54310");

    fs = FileSystem.get(conf);
    // #init-client

    // #init-mat
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
    // #init-mat
  }

  @AfterClass
  public static void teardown() throws Exception {
    fs.close();
    hdfsCluster.shutdown();
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void afterEach() throws IOException {
    fs.delete(new Path(destionation), true);
    fs.delete(settings.pathGenerator().apply(0L, 0L).getParent(), true);
  }

  private static void setupCluster() throws IOException {
    File baseDir = new File(JavaTestUtils.getTestDir(), "miniHDFS-java");
    HdfsConfiguration conf = new HdfsConfiguration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
    hdfsCluster = new MiniDFSCluster.Builder(conf).nameNodePort(54310).format(true).build();
    hdfsCluster.waitClusterUp();
  }

  private static void documentation() {
    // #define-generator
    BiFunction<Long, Long, String> func =
        (rotationCount, timestamp) -> "/tmp/alpakka/" + rotationCount + "-" + timestamp;
    FilePathGenerator pathGenerator = FilePathGenerator.create(func);
    // #define-generator
    // #define-settings
    HdfsWritingSettings.create()
        .withOverwrite(true)
        .withNewLine(false)
        .withPathGenerator(pathGenerator);
    // #define-settings
  }
}
