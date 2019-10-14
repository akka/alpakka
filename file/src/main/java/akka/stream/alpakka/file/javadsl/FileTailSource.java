/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.javadsl;

import akka.NotUsed;
import akka.actor.Cancellable;
import akka.japi.pf.PFBuilder;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import akka.util.JavaDurationConverters;
import scala.concurrent.duration.FiniteDuration;

import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Java API
 *
 * <p>Read the entire contents of a file, and then when the end is reached, keep reading newly
 * appended data. Like the unix command `tail -f`.
 *
 * <p>Aborting the stage can be done by combining with a [[akka.stream.KillSwitch]]
 *
 * <p>To use the stage from Scala see the factory methods in {@link
 * akka.stream.alpakka.file.scaladsl.FileTailSource}
 */
public final class FileTailSource {

  /**
   * Read the entire contents of a file as chunks of bytes and when the end is reached, keep reading
   * newly appended data. Like the unix command `tail -f` but for bytes.
   *
   * <p>Reading text lines can be done with the `createLines` factory methods or by composing with
   * other stages manually depending on your needs. Aborting the stage can be done by combining with
   * a [[akka.stream.KillSwitch]]
   *
   * @param path a file path to tail
   * @param maxChunkSize The max emitted size of the `ByteString`s
   * @param startingPosition Offset into the file to start reading
   * @param pollingInterval When the end has been reached, look for new content with this interval
   */
  public static Source<ByteString, NotUsed> create(
      Path path, int maxChunkSize, long startingPosition, java.time.Duration pollingInterval) {
    return Source.fromGraph(
        new akka.stream.alpakka.file.impl.FileTailSource(
            path,
            maxChunkSize,
            startingPosition,
            JavaDurationConverters.asFiniteDuration(pollingInterval)));
  }

  /**
   * Read the entire contents of a file as text lines, and then when the end is reached, keep
   * reading newly appended data. Like the unix command `tail -f`.
   *
   * <p>If a line is longer than `maxChunkSize` the stream will fail.
   *
   * <p>Aborting the stage can be done by combining with a [[akka.stream.KillSwitch]]
   *
   * @param path a file path to tail
   * @param maxLineSize The max emitted size of the `ByteString`s
   * @param pollingInterval When the end has been reached, look for new content with this interval
   * @param lf The character or characters used as line separator
   * @param charset The charset of the file
   */
  public static Source<String, NotUsed> createLines(
      Path path, int maxLineSize, java.time.Duration pollingInterval, String lf, Charset charset) {
    return create(path, maxLineSize, 0, pollingInterval)
        .via(Framing.delimiter(ByteString.fromString(lf, charset.name()), maxLineSize))
        .map(bytes -> bytes.decodeString(charset));
  }

  /**
   * Same as {@link #createLines(Path, int, java.time.Duration, String, Charset)} but using the OS
   * default line separator and UTF-8 for charset
   *
   * @deprecated (since 2.0.0) use method with `java.time.Duration` instead
   */
  @Deprecated
  public static Source<String, NotUsed> createLines(
      Path path, int maxChunkSize, FiniteDuration pollingInterval) {
    return createLines(
        path,
        maxChunkSize,
        java.time.Duration.ofNanos(pollingInterval.toNanos()),
        System.getProperty("line.separator"),
        StandardCharsets.UTF_8);
  }

  /**
   * Same as {@link #createLines(Path, int, java.time.Duration, String, Charset)} but using the OS
   * default line separator and UTF-8 for charset
   */
  public static Source<String, NotUsed> createLines(
      Path path, int maxChunkSize, java.time.Duration pollingInterval) {
    return createLines(
        path,
        maxChunkSize,
        pollingInterval,
        System.getProperty("line.separator"),
        StandardCharsets.UTF_8);
  }
}
