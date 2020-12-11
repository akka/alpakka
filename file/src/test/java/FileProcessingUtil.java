/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.Materializer;
import akka.stream.alpakka.file.TarArchiveMetadata;
import akka.stream.alpakka.file.javadsl.Archive;
import akka.stream.javadsl.*;
import akka.util.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

public class FileProcessingUtil {

  private static final int MAX_GUNZIP_CHUNK_SIZE = 64000;
  private static final String TARGZ_EXT = "tar.gz";
  private static final Logger log = LoggerFactory.getLogger(FileProcessingUtil.class);

  public static void main(String[] args) throws Exception {

    String filenameOk = "src/test/resources/container/lbktag_eakte_0000000015.tar";

    // String filename=filenameOk;
    String filename = "file/src/test/resources/nested-sample.tar";
    Path tempBase = Paths.get(System.getProperty("java.io.tmpdir"));
    File tmpDir = tempBase.resolve(FileProcessingUtil.class.getSimpleName()).toFile();
    Path outDir = tmpDir.toPath().resolve("out");
    if (tmpDir.exists() && !tmpDir.isDirectory()) {
      tmpDir.delete();
    }
    Path file = Paths.get(filename);
    ActorSystem system = ActorSystem.create("test");
    Materializer mat = Materializer.matFromSystem(system);
    FileProcessingUtil.process(file, outDir, mat)
        .thenAccept(
            done -> {
              system.terminate();
            });
  }

  public static CompletionStage<Done> process(Path filename, Path targetDir, Materializer mat) {
    return FileIO.fromPath(filename)
        .via(unTarFlow(targetDir, mat, 0))
        .log("process")
        .runWith(Sink.ignore(), mat);
  }

  private static Flow<ByteString, TarArchiveMetadata, NotUsed> unTarFlow(
      Path targetDir, Materializer mat, int level) {
    return Archive.tarReader()
        .log("unTarFlow (level=" + level + ")")
        .map(
            pair -> {
              System.out.println("metadataPath (level=" + level + "): " + pair.first().filePath());
              return pair;
            })
        .mapAsync(
            1,
            pair -> {
              TarArchiveMetadata metadata = pair.first();
              Source<ByteString, NotUsed> source = pair.second();
              Path targetFile = targetDir.resolve(metadata.filePath());

              return getSourceShapeObjectGraph(metadata, source, targetFile, mat, level)
                  .run(mat)
                  .thenApply(d -> metadata);
            })
        .log("unTarFlow after  (level=" + level + ")");
  }

  private static RunnableGraph<CompletionStage<Done>> getSourceShapeObjectGraph(
      TarArchiveMetadata metadata,
      Source<ByteString, NotUsed> source,
      Path targetFile,
      Materializer mat,
      int level) {
    if (metadata.isDirectory()) {
      return Source.single(targetFile)
          .filter(t -> (!t.toFile().exists() || !t.toFile().isDirectory()))
          // .via(Directory.mkdirs())
          .log("unTarFlow dir")
          .toMat(Sink.ignore(), Keep.right());
    } else {
      if (targetFile.getFileName().toString().endsWith(TARGZ_EXT)) {
        Path targetSubDir =
            targetFile
                .getParent()
                .resolve(
                    Paths.get(
                        targetFile.getFileName().toString().substring(0, TARGZ_EXT.length() - 2)
                            + "/"));
        return source
            .via(Compression.gunzip(MAX_GUNZIP_CHUNK_SIZE))
            //                        .log("pre unTar (level="+level+")")
            .via(unTarFlow(targetSubDir, mat, level + 1))
            //                        .log("unTarFlow uncompress (level="+level+")")
            .toMat(Sink.ignore(), Keep.right());
      } else
        /*return source.log("unTarFlow save file").runWith(FileIO.toPath(targetFile), mat)
        .thenApply(res-> Done.getInstance());*/
        return source.toMat(Sink.ignore(), Keep.right());
    }
  }
}
