/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.alpakka.file.ArchiveMetadata;
import akka.stream.alpakka.file.ArchiveMetadataWithSize;
import akka.stream.alpakka.file.javadsl.Archive;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.*;
import static akka.util.ByteString.emptyByteString;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ArchiveTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static Materializer mat;

  @BeforeClass
  public static void beforeAll() throws Exception {
    system = ActorSystem.create();
    mat = ActorMaterializer.create(system);
  }

  @AfterClass
  public static void afterAll() throws Exception {
    TestKit.shutdownActorSystem(system);
  }

  private ArchiveHelper archiveHelper;

  @Before
  public void setup() throws Exception {
    archiveHelper = new ArchiveHelper();
  }

  @Test
  public void flowShouldCreateZIPArchive() throws Exception {
    ByteString fileContent1 = readFileAsByteString(getFileFromResource("akka_full_color.svg"));
    ByteString fileContent2 = readFileAsByteString(getFileFromResource("akka_icon_reverse.svg"));

    Source<ByteString, NotUsed> source1 = toSource(fileContent1);
    Source<ByteString, NotUsed> source2 = toSource(fileContent2);

    /*
    // #sample-zip
    Source<ByteString, NotUsed> source1 = ...
    Source<ByteString, NotUsed> source2 = ...

    // #sample-zip
    */

    // #sample-zip
    Pair<ArchiveMetadata, Source<ByteString, NotUsed>> pair1 =
        Pair.create(ArchiveMetadata.create("akka_full_color.svg"), source1);
    Pair<ArchiveMetadata, Source<ByteString, NotUsed>> pair2 =
        Pair.create(ArchiveMetadata.create("akka_icon_reverse.svg"), source2);

    Source<Pair<ArchiveMetadata, Source<ByteString, NotUsed>>, NotUsed> source =
        Source.from(Arrays.asList(pair1, pair2));

    Sink<ByteString, CompletionStage<IOResult>> fileSink = FileIO.toPath(Paths.get("logo.zip"));
    CompletionStage<IOResult> ioResult = source.via(Archive.zip()).runWith(fileSink, mat);

    // #sample-zip

    ioResult.toCompletableFuture().get(3, TimeUnit.SECONDS);

    Map<String, ByteString> inputFiles =
        new HashMap<String, ByteString>() {
          {
            put("akka_full_color.svg", fileContent1);
            put("akka_icon_reverse.svg", fileContent2);
          }
        };

    ByteString resultFileContent = readFileAsByteString(Paths.get("logo.zip"));
    Map<String, ByteString> unzip = archiveHelper.unzip(resultFileContent);

    assertEquals(inputFiles, unzip);

    // cleanup
    new File("logo.zip").delete();
  }

  @Test
  public void flowShouldCreateTARArchive() throws Exception {
    Path filePath1 = getFileFromResource("akka_full_color.svg");
    Path filePath2 = getFileFromResource("akka_icon_reverse.svg");

    ByteString fileContent1 = readFileAsByteString(filePath1);
    ByteString fileContent2 = readFileAsByteString(filePath2);

    Source<ByteString, NotUsed> source1 = toSource(fileContent1);
    Long size1 = Files.size(filePath1);
    Source<ByteString, NotUsed> source2 = toSource(fileContent2);
    Long size2 = Files.size(filePath2);

    /*
    // #sample-tar
    Source<ByteString, NotUsed> source1 = ...
    Source<ByteString, NotUsed> source2 = ...
    Long size1 = ...
    Long size2 = ...

    // #sample-tar
    */

    // #sample-tar
    Pair<ArchiveMetadataWithSize, Source<ByteString, NotUsed>> pair1 =
        Pair.create(ArchiveMetadataWithSize.create("akka_full_color.svg", size1), source1);
    Pair<ArchiveMetadataWithSize, Source<ByteString, NotUsed>> pair2 =
        Pair.create(ArchiveMetadataWithSize.create("akka_icon_reverse.svg", size2), source2);

    Source<Pair<ArchiveMetadataWithSize, Source<ByteString, NotUsed>>, NotUsed> source =
        Source.from(Arrays.asList(pair1, pair2));

    Sink<ByteString, CompletionStage<IOResult>> fileSink = FileIO.toPath(Paths.get("logo.tar"));
    CompletionStage<IOResult> ioResult = source.via(Archive.tar()).runWith(fileSink, mat);
    // #sample-tar

    // #sample-tar-gz
    Sink<ByteString, CompletionStage<IOResult>> fileSinkGz =
        FileIO.toPath(Paths.get("logo.tar.gz"));
    CompletionStage<IOResult> ioResultGz =
        source
            .via(Archive.tar().via(akka.stream.javadsl.Compression.gzip()))
            .runWith(fileSinkGz, mat);
    // #sample-tar-gz

    ioResult.toCompletableFuture().get(3, TimeUnit.SECONDS);
    ioResultGz.toCompletableFuture().get(3, TimeUnit.SECONDS);

    // cleanup
    new File("logo.tar").delete();
    new File("logo.tar.gz").delete();
  }

  @After
  public void tearDown() throws Exception {
    StreamTestKit.assertAllStagesStopped(mat);
  }

  private Path getFileFromResource(String fileName) {
    return Paths.get(getClass().getClassLoader().getResource(fileName).getPath());
  }

  private ByteString readFileAsByteString(Path filePath) throws Exception {
    final Sink<ByteString, CompletionStage<ByteString>> foldSink =
        Sink.fold(emptyByteString(), ByteString::concat);

    return FileIO.fromPath(filePath)
        .runWith(foldSink, mat)
        .toCompletableFuture()
        .get(3, TimeUnit.SECONDS);
  }

  private Source<ByteString, NotUsed> toSource(ByteString bs) {
    return Source.single(bs);
  }
}
