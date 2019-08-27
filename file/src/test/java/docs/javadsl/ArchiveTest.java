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
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.testkit.javadsl.StreamTestKit;
import akka.testkit.TestKit;
import akka.util.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;
import static akka.util.ByteString.emptyByteString;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ArchiveTest {
  private ActorSystem system;
  private Materializer mat;
  private ArchiveHelper archiveHelper;

  @Before
  public void setup() throws Exception {
    system = ActorSystem.create();
    mat = ActorMaterializer.create(system);
    archiveHelper = new ArchiveHelper();
  }

  @Test
  public void flowShouldCreateZIPArchive() throws Exception {
    Path filePath1 = getFileFromResource("akka_full_color.svg");
    Path filePath2 = getFileFromResource("akka_icon_reverse.svg");

    Source<ByteString, NotUsed> source1 =
        FileIO.fromPath(filePath1).mapMaterializedValue((v) -> NotUsed.getInstance());
    Source<ByteString, NotUsed> source2 =
        FileIO.fromPath(filePath2).mapMaterializedValue((v) -> NotUsed.getInstance());

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

    archiveHelper.createReferenceZipFile(Arrays.asList(filePath1, filePath2), "logo-reference.zip");

    final Sink<ByteString, CompletionStage<ByteString>> foldSink =
        Sink.fold(emptyByteString(), ByteString::concat);

    ByteString resultFileContent =
        FileIO.fromPath(Paths.get("logo.zip"))
            .runWith(foldSink, mat)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);
    ByteString referenceFileContent =
        FileIO.fromPath(Paths.get("logo-reference.zip"))
            .runWith(foldSink, mat)
            .toCompletableFuture()
            .get(3, TimeUnit.SECONDS);

    assertEquals(resultFileContent, referenceFileContent);

    // cleanup
    new File("logo.zip").delete();
    new File("logo-reference.zip").delete();
  }

  @After
  public void tearDown() throws Exception {
    StreamTestKit.assertAllStagesStopped(mat);
    TestKit.shutdownActorSystem(system, FiniteDuration.apply(3, TimeUnit.SECONDS), true);
  }

  private Path getFileFromResource(String fileName) {
    return Paths.get(getClass().getClassLoader().getResource(fileName).getPath());
  }
}
