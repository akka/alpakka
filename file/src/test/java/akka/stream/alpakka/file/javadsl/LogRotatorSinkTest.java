/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl;

import akka.Done;
import akka.actor.ActorSystem;
import akka.japi.function.Creator;
import akka.japi.function.Function;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;

import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;


public class LogRotatorSinkTest {

    public static void main(String... args) {
        final ActorSystem system = ActorSystem.create();
        final Materializer materializer = ActorMaterializer.create(system);

        // #size
        Creator<Function<ByteString, Optional<Path>>> sizeBasedPathGenerator = () -> {
            long max = 10 * 1024 * 1024;
            final long[] size = new long[]{max};
            return (element) -> {
                if (size[0] + element.size() > max) {
                    Path path = Files.createTempFile("out-", ".log");
                    size[0] = element.size();
                    return Optional.of(path);
                } else {
                    size[0] += element.size();
                    return Optional.empty();
                }
            };
        };

        Sink<ByteString, CompletionStage<Done>> sizeRotatorSink =
                LogRotatorSink.createFromFunction(sizeBasedPathGenerator);
        // #size

        // #time
        final Path destinationDir = FileSystems.getDefault().getPath("/tmp");
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("'stream-'yyyy-MM-dd_HH'.log'");

        Creator<Function<ByteString, Optional<Path>>> timeBasedPathCreator = () -> {
            final String[] currentFileName = new String[]{null};
            return (element) -> {
                String newName = LocalDateTime.now().format(formatter);
                if (newName.equals(currentFileName[0])) {
                    return Optional.empty();
                } else {
                    currentFileName[0] = newName;
                    return Optional.of(destinationDir.resolve(newName));
                }
            };
        };

        Sink<ByteString, CompletionStage<Done>> timeBaseSink =
                LogRotatorSink.createFromFunction(timeBasedPathCreator);
        // #time

        /*
        // #sample
        import akka.stream.alpakka.file.javadsl.LogRotatorSink;

        Creator<Function<ByteString, Optional<Path>>> pathGeneratorCreator = ...;

        // #sample
        */
        Creator<Function<ByteString, Optional<Path>>> pathGeneratorCreator = timeBasedPathCreator;
        // #sample
        CompletionStage<Done> completion = Source.from(Arrays.asList("test1", "test2", "test3", "test4", "test5", "test6"))
                .map(ByteString::fromString)
                .runWith(LogRotatorSink.createFromFunction(pathGeneratorCreator), materializer);
        // #sample

        completion.thenRun(() -> system.terminate());
    }
}
