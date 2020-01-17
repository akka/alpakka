/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.alpakka.hdfs.*;
import akka.stream.alpakka.hdfs.javadsl.HdfsFlow;
import akka.stream.alpakka.hdfs.javadsl.HdfsSource;
import akka.stream.alpakka.hdfs.util.JavaTestUtils;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;

public class HdfsReaderTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static MiniDFSCluster hdfsCluster = null;
  private static ActorSystem system;
  private static ActorMaterializer materializer;
  private static String destination = JavaTestUtils.destination();
  private static FileSystem fs = null;
  private static HdfsWritingSettings settings = HdfsWritingSettings.create();

  @Test
  public void testReadDataFile() throws Exception {
    List<ByteString> data = JavaTestUtils.generateFakeContent(1.0, FileUnit.KB().byteCount());

    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.data(
            fs, SyncStrategy.count(500), RotationStrategy.size(0.5, FileUnit.KB()), settings);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(data).map(HdfsWriteMessage::create).via(flow).runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<Character> readData = new ArrayList<>();

    for (RotationMessage log : logs) {
      Path path = new Path("/tmp/alpakka", log.path());
      // #define-data-source
      Source<ByteString, CompletionStage<IOResult>> source = HdfsSource.data(fs, path);
      // #define-data-source
      ArrayList<ByteString> result =
          new ArrayList<>(source.runWith(Sink.seq(), materializer).toCompletableFuture().get());
      for (ByteString bs : result) {
        readData.addAll(
            bs.utf8String().chars().mapToObj(i -> (char) i).collect(Collectors.toList()));
      }
    }

    assertArrayEquals(
        readData.toArray(),
        data.stream().flatMap(bs -> bs.utf8String().chars().mapToObj(i -> (char) i)).toArray());
  }

  @Test
  public void testCompressedDataFile() throws Exception {
    DefaultCodec codec = new DefaultCodec();
    codec.setConf(fs.getConf());

    Flow<HdfsWriteMessage<ByteString, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.compressed(
            fs, SyncStrategy.count(1), RotationStrategy.size(0.1, FileUnit.MB()), codec, settings);

    List<ByteString> content =
        JavaTestUtils.generateFakeContentWithPartitions(1, FileUnit.MB().byteCount(), 30);

    CompletionStage<List<RotationMessage>> resF =
        Source.from(content)
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<Character> readData = new ArrayList<>();

    for (RotationMessage log : logs) {
      Path path = new Path("/tmp/alpakka", log.path());
      // #define-compressed-source
      Source<ByteString, CompletionStage<IOResult>> source = HdfsSource.compressed(fs, path, codec);
      // #define-compressed-source
      ArrayList<ByteString> result =
          new ArrayList<>(source.runWith(Sink.seq(), materializer).toCompletableFuture().get());
      for (ByteString bs : result) {
        readData.addAll(
            bs.utf8String().chars().mapToObj(i -> (char) i).collect(Collectors.toList()));
      }
    }

    assertArrayEquals(
        readData.toArray(),
        content.stream().flatMap(bs -> bs.utf8String().chars().mapToObj(i -> (char) i)).toArray());
  }

  @Test
  public void testReadSequenceFile() throws Exception {
    Flow<HdfsWriteMessage<Pair<Text, Text>, NotUsed>, RotationMessage, NotUsed> flow =
        HdfsFlow.sequence(
            fs,
            SyncStrategy.none(),
            RotationStrategy.size(1, FileUnit.MB()),
            settings,
            Text.class,
            Text.class);

    List<Pair<Text, Text>> content =
        JavaTestUtils.generateFakeContentForSequence(0.5, FileUnit.MB().byteCount());

    CompletionStage<List<RotationMessage>> resF =
        Source.from(content)
            .map(HdfsWriteMessage::create)
            .via(flow)
            .runWith(Sink.seq(), materializer);

    List<RotationMessage> logs = new ArrayList<>(resF.toCompletableFuture().get());
    List<Pair<Text, Text>> readData = new ArrayList<>();

    for (RotationMessage log : logs) {
      Path path = new Path("/tmp/alpakka", log.path());
      // #define-sequence-source
      Source<Pair<Text, Text>, NotUsed> source =
          HdfsSource.sequence(fs, path, Text.class, Text.class);
      // #define-sequence-source
      ArrayList<Pair<Text, Text>> result =
          new ArrayList<>(source.runWith(Sink.seq(), materializer).toCompletableFuture().get());
      readData.addAll(result);
    }

    assertArrayEquals(readData.toArray(), content.toArray());
  }

  @BeforeClass
  public static void setup() throws Exception {
    hdfsCluster = JavaTestUtils.setupCluster();

    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://localhost:54310");

    fs = FileSystem.get(conf);

    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);
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
}
