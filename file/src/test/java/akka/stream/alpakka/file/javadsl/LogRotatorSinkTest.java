/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.util.ByteString;
import scala.Function0;
import scala.Function1;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;


public class LogRotatorSinkTest {

    public static void main(String... args) {
        final ActorSystem system = ActorSystem.create();
        final Materializer materializer = ActorMaterializer.create(system);

        // #LogRotationSink-filesize-sample
        final FileSystem fs = FileSystems.getDefault();

        Sink<ByteString, CompletionStage<Done>> fileSizeBasedRotation = LogRotatorSink.createFromFunction(
                () -> {
                    long max = 10 * 1024 * 1024;
                    final long[] size = new long[] {max};
                    return (element) -> {
                        if (size[0] + element.size() > max) {
                            Path path = Files.createTempFile(fs.getPath("/"), "test", ".log");
                            size[0] = element.size();
                            return path;
                        } else {
                            size[0] += element.size();
                            return null;
                        }
                    };
                });
        // #LogRotationSink-filesize-sample

        // #LogRotationSink-timebased-sample
        Sink<ByteString, CompletionStage<Done>> timeBasedRotation = LogRotatorSink.createFromFunction(
                () -> {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
                    final String[] lastFileName = new String[] {null};
                    return (element) -> {
                        String newName = ZonedDateTime.now().format(formatter) + ".log";
                        if (lastFileName[0] == null || !lastFileName[0].equals(newName)) {
                            lastFileName[0] = newName;
                            return fs.getPath(newName);
                        } else {
                            return null;
                        }
                    };
                });
        // #LogRotationSink-timebased-sample

    }
}
