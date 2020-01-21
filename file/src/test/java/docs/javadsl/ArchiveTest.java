/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.alpakka.file.ArchiveMetadata;
import akka.stream.alpakka.file.javadsl.Archive;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import org.junit.*;
import scala.concurrent.duration.FiniteDuration;
import static akka.util.ByteString.emptyByteString;
import java.io.File;
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
    // #sample
    Source<ByteString, NotUsed> source1 = ...
    Source<ByteString, NotUsed> source2 = ...
    // #sample
    */

    // #sample
    Pair<ArchiveMetadata, Source<ByteString, NotUsed>> pair1 =
        Pair.create(ArchiveMetadata.create("akka_full_color.svg"), source1);
    Pair<ArchiveMetadata, Source<ByteString, NotUsed>> pair2 =
        Pair.create(ArchiveMetadata.create("akka_icon_reverse.svg"), source2);

    Source<Pair<ArchiveMetadata, Source<ByteString, NotUsed>>, NotUsed> source =
        Source.from(Arrays.asList(pair1, pair2));

    Sink<ByteString, CompletionStage<IOResult>> fileSink = FileIO.toPath(Paths.get("logo.zip"));
    CompletionStage<IOResult> ioResult = source.via(Archive.zip()).runWith(fileSink, mat);

    // #sample

    ioResult.toCompletableFuture().get(3, TimeUnit.SECONDS);

    Map<String, ByteString> inputFiles =
        new HashMap<String, ByteString>() {
          {
            put("akka_full_color.svg", fileContent1);
            put("akka_icon_reverse.svg", fileContent2);
          }
        };

    archiveHelper.createReferenceZipFileFromMemory(inputFiles, "logo-reference.zip");

    ByteString resultFileContent = readFileAsByteString(Paths.get("logo.zip"));
    ByteString referenceFileContent = readFileAsByteString(Paths.get("logo-reference.zip"));

    assertEquals(resultFileContent, referenceFileContent);

    // cleanup
    new File("logo.zip").delete();
    new File("logo-reference.zip").delete();
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
