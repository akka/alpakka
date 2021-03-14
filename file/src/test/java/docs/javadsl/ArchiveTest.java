/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.alpakka.file.ArchiveMetadata;
import akka.stream.alpakka.file.TarArchiveMetadata;
import akka.stream.alpakka.file.javadsl.Archive;
import akka.stream.alpakka.file.javadsl.Directory;
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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;

public class ArchiveTest {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;

  @BeforeClass
  public static void beforeAll() throws Exception {
    system = ActorSystem.create();
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
    CompletionStage<IOResult> ioResult = source.via(Archive.zip()).runWith(fileSink, system);

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

    assertThat(inputFiles, is(unzip));

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

    Instant lastModification = Instant.now();
    // #sample-tar
    Pair<TarArchiveMetadata, Source<ByteString, NotUsed>> dir =
        Pair.create(TarArchiveMetadata.directory("subdir", lastModification), Source.empty());

    Pair<TarArchiveMetadata, Source<ByteString, NotUsed>> pair1 =
        Pair.create(
            TarArchiveMetadata.create("subdir", "akka_full_color.svg", size1, lastModification),
            source1);
    Pair<TarArchiveMetadata, Source<ByteString, NotUsed>> pair2 =
        Pair.create(
            TarArchiveMetadata.create("akka_icon_reverse.svg", size2, lastModification), source2);

    Source<Pair<TarArchiveMetadata, Source<ByteString, NotUsed>>, NotUsed> source =
        Source.from(Arrays.asList(dir, pair1, pair2));

    Sink<ByteString, CompletionStage<IOResult>> fileSink = FileIO.toPath(Paths.get("logo.tar"));
    CompletionStage<IOResult> ioResult = source.via(Archive.tar()).runWith(fileSink, system);
    // #sample-tar

    // #sample-tar-gz
    Sink<ByteString, CompletionStage<IOResult>> fileSinkGz =
        FileIO.toPath(Paths.get("logo.tar.gz"));
    CompletionStage<IOResult> ioResultGz =
        source
            .via(Archive.tar().via(akka.stream.javadsl.Compression.gzip()))
            .runWith(fileSinkGz, system);
    // #sample-tar-gz

    ioResult.toCompletableFuture().get(3, TimeUnit.SECONDS);
    ioResultGz.toCompletableFuture().get(3, TimeUnit.SECONDS);

    // cleanup
    new File("logo.tar").delete();
    new File("logo.tar.gz").delete();
  }

  @Test
  public void tarReader() throws Exception {
    ByteString tenDigits = ByteString.fromString("1234567890");
    TarArchiveMetadata metadata1 = TarArchiveMetadata.directory("dir/");
    TarArchiveMetadata metadata2 = TarArchiveMetadata.create("dir/file1.txt", tenDigits.length());
    CompletionStage<ByteString> oneFileArchive =
        Source.from(
                Arrays.asList(
                    Pair.create(metadata1, Source.empty(ByteString.class)),
                    Pair.create(metadata2, Source.single(tenDigits))))
            .via(Archive.tar())
            .runWith(Sink.fold(ByteString.emptyByteString(), ByteString::concat), system);

    // #tar-reader
    Source<ByteString, NotUsed> bytesSource = // ???
        // #tar-reader
        Source.completionStage(oneFileArchive);
    Path target = Files.createTempDirectory("alpakka-tar-");

    // #tar-reader
    CompletionStage<Done> tar =
        bytesSource
            .via(Archive.tarReader())
            .mapAsync(
                1,
                pair -> {
                  TarArchiveMetadata metadata = pair.first();
                  Path targetFile = target.resolve(metadata.filePath());
                  if (metadata.isDirectory()) {
                    return Source.single(targetFile)
                        .via(Directory.mkdirs())
                        .runWith(Sink.ignore(), system);
                  } else {
                    Source<ByteString, NotUsed> source = pair.second();
                    // create the target directory
                    return Source.single(targetFile.getParent())
                        .via(Directory.mkdirs())
                        .runWith(Sink.ignore(), system)
                        .thenCompose(
                            done ->
                                // stream the file contents to a local file
                                source
                                    .runWith(FileIO.toPath(targetFile), system)
                                    .thenApply(io -> Done.done()));
                  }
                })
            .runWith(Sink.ignore(), system);
    // #tar-reader
    assertThat(tar.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.done()));
    File file = target.resolve("dir/file1.txt").toFile();
    assertThat(file.exists(), is(true));
  }

  @Test
  public void tarReaderWithEofBlocks() throws Exception {
    ByteString tenDigits = ByteString.fromString("1234567890");
    TarArchiveMetadata metadata1 = TarArchiveMetadata.create("file5.txt", tenDigits.length());
    CompletionStage<ByteString> oneFileArchive =
        Source.single(Pair.create(metadata1, Source.single(tenDigits)))
            .via(Archive.tar())
            // tar standard suggests two empty 512 byte blocks as EOF marker
            .concat(Source.single(ByteString.fromArray(new byte[1024])))
            .runWith(Sink.fold(ByteString.emptyByteString(), ByteString::concat), system);

    Source<ByteString, NotUsed> bytesSource = Source.completionStage(oneFileArchive);
    Path target = Files.createTempDirectory("alpakka-tar-");

    CompletionStage<Done> tar =
        bytesSource
            .via(Archive.tarReader())
            .mapAsync(
                1,
                pair -> {
                  TarArchiveMetadata metadata = pair.first();
                  Source<ByteString, NotUsed> source = pair.second();
                  Path targetFile = target.resolve(metadata.filePath());
                  return source.runWith(FileIO.toPath(targetFile), system);
                })
            .runWith(Sink.ignore(), system);
    assertThat(tar.toCompletableFuture().get(2, TimeUnit.SECONDS), is(Done.done()));
    File file = target.resolve("file5.txt").toFile();
    assertThat(file.exists(), is(true));
  }

  @After
  public void tearDown() throws Exception {
    StreamTestKit.assertAllStagesStopped(Materializer.matFromSystem(system));
  }

  private Path getFileFromResource(String fileName) throws URISyntaxException {
    return Paths.get(getClass().getClassLoader().getResource(fileName).toURI());
  }

  private ByteString readFileAsByteString(Path filePath) throws Exception {
    final Sink<ByteString, CompletionStage<ByteString>> foldSink =
        Sink.fold(emptyByteString(), ByteString::concat);

    return FileIO.fromPath(filePath)
        .runWith(foldSink, system)
        .toCompletableFuture()
        .get(3, TimeUnit.SECONDS);
  }

  private Source<ByteString, NotUsed> toSource(ByteString bs) {
    return Source.single(bs);
  }
}
