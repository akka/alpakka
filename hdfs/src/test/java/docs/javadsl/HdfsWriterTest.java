/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.actor.Cancellable;
import akka.japi.Pair;
import akka.stream.alpakka.hdfs.*;
import akka.stream.alpakka.hdfs.javadsl.HdfsFlow;
import akka.stream.alpakka.hdfs.util.JavaTestUtils;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.*;
import scala.concurrent.duration.Duration;

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
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static MiniDFSCluster hdfsCluster = null;
  private static ActorSystem system;
  private static String destination = JavaTestUtils.destination();
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
    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(
            fs, SyncStrategy.count(50), RotationStrategy.size(0.01, FileUnit.KB()), settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(HdfsWriteMessage::create).via(flow).runWith(Sink.seq(), system);

    List<RotationMessage> result = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Arrays.asList(
            new RotationMessage(output("0"), 0),
            new RotationMessage(output("1"), 1),
            new RotationMessage(output("2"), 2),
            new RotationMessage(output("3"), 3),
            new RotationMessage(output("4"), 4));

    assertEquals(expect, result);
  }

  @Test
  public void testDataWriterFileSizeRotationWithTwoFile() throws Exception {
    List<ByteString> data = JavaTestUtils.generateFakeContent(1.0, FileUnit.KB().byteCount());

    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(
            fs, SyncStrategy.count(500), RotationStrategy.size(0.5, FileUnit.KB()), settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(data).map(HdfsWriteMessage::create).via(flow).runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    assertEquals(logs.size(), 2);

    List<FileStatus> files = JavaTestUtils.getFiles(fs);
    assertEquals(
        files.stream().map(FileStatus::getLen).reduce((a, b) -> a + b).get().longValue(), 1024L);
    files.forEach(file -> assertTrue(file.getLen() <= 512L));
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyFlattenContent(fs, logs, data);
  }

  @Test
  public void testDataWriterDetectUpstreamFinish() throws Exception {
    List<ByteString> data = JavaTestUtils.generateFakeContent(1.0, FileUnit.KB().byteCount());

    // #define-data
    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(
            fs, SyncStrategy.count(500), RotationStrategy.size(1, FileUnit.GB()), settings);
    // #define-data
    CompletionStage<List<RotationMessage>> resF =
        Source.from(data).map(HdfsWriteMessage::create).via(flow).runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    assertEquals(logs.size(), 1);

    assertEquals(JavaTestUtils.getFiles(fs).get(0).getLen(), 1024L);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyFlattenContent(fs, logs, data);
  }

  @Test
  public void testDataWriterWithBufferRotation() throws Exception {
    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(fs, SyncStrategy.count(1), RotationStrategy.count(2), settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(HdfsWriteMessage::create).via(flow).runWith(Sink.seq(), system);

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
            .map(HdfsWriteMessage::create)
            .via(
                HdfsFlow.data(
                    fs,
                    SyncStrategy.none(),
                    RotationStrategy.time(Duration.create(500, TimeUnit.MILLISECONDS)),
                    settings))
            .toMat(Sink.seq(), Keep.both())
            .run(system);

    system
        .scheduler()
        .scheduleOnce(
            java.time.Duration.ofMillis(1500),
            () -> resF.first().cancel(),
            system.dispatcher()); // cancel within 1500 milliseconds

    List<RotationMessage> logs = new ArrayList<>(resF.second().toCompletableFuture().get());
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    assertTrue(logs.size() == 3 || logs.size() == 4);
  }

  @Test
  public void testDataWriterWithNoRotation() throws Exception {
    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(fs, SyncStrategy.none(), RotationStrategy.none(), settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(HdfsWriteMessage::create).via(flow).runWith(Sink.seq(), system);

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

    Flow<HdfsWriteMessage<ByteString, KafkaOffset>, OutgoingMessage<KafkaOffset>, NotUsed> flow =
        HdfsFlow.dataWithPassThrough(
            fs,
            SyncStrategy.count(50),
            RotationStrategy.count(4),
            HdfsWritingSettings.create().withNewLine(true));

    CompletionStage<List<RotationMessage>> resF =
        Source.from(messagesFromKafka)
            .map(
                kafkaMessage -> {
                  Book book = kafkaMessage.book;
                  // Transform message so that we can write to hdfs\
                  return HdfsWriteMessage.create(
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
            .runWith(Sink.seq(), system);
    // #kafka-example

    ArrayList<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Arrays.asList(new RotationMessage(output("0"), 0), new RotationMessage(output("1"), 1));

    // Make sure all messages was committed to kafka
    assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5), kafkaCommitter.committedOffsets);

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    assertEquals(
        JavaTestUtils.readLogs(fs, logs).stream()
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
    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.compressed(
            fs, SyncStrategy.count(50), RotationStrategy.size(0.1, FileUnit.MB()), codec, settings);
    // #define-compress

    List<ByteString> content =
        JavaTestUtils.generateFakeContentWithPartitions(1, FileUnit.MB().byteCount(), 30);

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Arrays.asList(
            new RotationMessage(output("0.deflate"), 0),
            new RotationMessage(output("1.deflate"), 1),
            new RotationMessage(output("2.deflate"), 2),
            new RotationMessage(output("3.deflate"), 3),
            new RotationMessage(output("4.deflate"), 4),
            new RotationMessage(output("5.deflate"), 5));

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyLogsWithCodec(fs, content, logs, codec);
  }

  @Test
  public void testCompressedDataWriterWithBufferRotation() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());

    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.compressed(fs, SyncStrategy.count(1), RotationStrategy.count(1), codec, settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(books).map(HdfsWriteMessage::create).via(flow).runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Arrays.asList(
            new RotationMessage(output("0.deflate"), 0),
            new RotationMessage(output("1.deflate"), 1),
            new RotationMessage(output("2.deflate"), 2),
            new RotationMessage(output("3.deflate"), 3),
            new RotationMessage(output("4.deflate"), 4));

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyLogsWithCodec(fs, books, logs, codec);
  }

  @Test
  public void testCompressedDataWriterWithNoRotation() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());

    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.compressed(fs, SyncStrategy.none(), RotationStrategy.none(), codec, settings);

    List<ByteString> content =
        JavaTestUtils.generateFakeContentWithPartitions(1, FileUnit.MB().byteCount(), 30);

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<RotationMessage> expect =
        Collections.singletonList(new RotationMessage(output("0.deflate"), 0));

    assertEquals(logs, expect);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifyLogsWithCodec(fs, content, logs, codec);
  }

  @Test
  public void testSequenceWriterWithSizeRotationWithoutCompression() throws Exception {
    // #define-sequence-compressed
    Flow<HdfsWriteMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs,
            SyncStrategy.none(),
            RotationStrategy.size(1, FileUnit.MB()),
            settings,
            Text.class,
            Text.class);
    // #define-sequence-compressed

    List<Pair<Text, Text>> content =
        JavaTestUtils.generateFakeContentForSequence(0.5, FileUnit.MB().byteCount());

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @Test
  public void testSequenceWriterWithSizeRotationWithCompression() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());

    // #define-sequence
    Flow<HdfsWriteMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs,
            SyncStrategy.none(),
            RotationStrategy.size(1, FileUnit.MB()),
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
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @Test
  public void testSequenceWriterWithBufferRotation() throws Exception {
    Flow<HdfsWriteMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs, SyncStrategy.none(), RotationStrategy.count(1), settings, Text.class, Text.class);

    List<Pair<Text, Text>> content = JavaTestUtils.booksForSequenceWriter();

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    assertEquals(logs.size(), 5);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @Test
  public void testSequenceWriterWithNoRotation() throws Exception {
    Flow<HdfsWriteMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs, SyncStrategy.none(), RotationStrategy.none(), settings, Text.class, Text.class);

    List<Pair<Text, Text>> content =
        JavaTestUtils.generateFakeContentForSequence(0.5, FileUnit.MB().byteCount());

    CompletionStage<List<RotationMessage>> resF =
        Source.fromIterator(content::iterator)
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), system);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());

    assertEquals(logs.size(), 1);
    JavaTestUtils.verifyOutputFileSize(fs, logs);
    JavaTestUtils.verifySequenceFile(fs, content, logs);
  }

  @BeforeClass
  public static void setup() throws Exception {
    hdfsCluster = JavaTestUtils.setupCluster();

    // #init-client
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://localhost:54310");

    fs = FileSystem.get(conf);
    // #init-client

    system = ActorSystem.create();
  }

  @AfterClass
  public static void teardown() throws Exception {
    fs.close();
    hdfsCluster.shutdown();
    TestKit.shutdownActorSystem(system);
  }

  @After
  public void afterEach() throws IOException {
    fs.delete(new Path(destination), true);
    fs.delete(settings.pathGenerator().apply(0L, 0L).getParent(), true);
  }

  private static String output(String s) {
    return destination + s;
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
        .withLineSeparator(System.getProperty("line.separator"))
        .withPathGenerator(pathGenerator);
    // #define-settings
  }
}
