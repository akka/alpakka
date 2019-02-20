/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl;

import akka.NotUsed;
import akka.annotation.InternalApi;
import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.alpakka.file.DirectoryChange;
import akka.stream.javadsl.Source;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.TimerGraphStageLogic;
import com.sun.nio.file.SensitivityWatchEventModifier;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.BiFunction;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * INTERNAL API
 *
 * <p>Watches a file system directory and streams change events from it.
 *
 * <p>Note that the JDK watcher is notoriously slow on some platform (up to 1s after event actually
 * happened on OSX for example)
 */
@InternalApi
public final class DirectoryChangesSource<T> extends GraphStage<SourceShape<T>> {

  private static final Attributes DEFAULT_ATTRIBUTES = Attributes.name("DirectoryChangesSource");

  private final Path directoryPath;
  private final FiniteDuration pollInterval;
  private final int maxBufferSize;
  private final BiFunction<Path, DirectoryChange, T> combiner;
  public final Outlet<T> out = Outlet.create("DirectoryChangesSource.out");
  private final SourceShape<T> shape = SourceShape.of(out);

  /**
   * @param directoryPath Directory to watch
   * @param pollInterval Interval between polls to the JDK watch service when a push comes in and
   *     there was no changes, if the JDK implementation is slow, it will not help lowering this
   * @param maxBufferSize Maximum number of buffered directory changes before the stage fails
   * @param combiner A function that combines a Path and a DirectoryChange into an element that will
   *     be emitted downstream
   */
  public DirectoryChangesSource(
      Path directoryPath,
      FiniteDuration pollInterval,
      int maxBufferSize,
      BiFunction<Path, DirectoryChange, T> combiner) {
    this.directoryPath = directoryPath;
    this.pollInterval = pollInterval;
    this.maxBufferSize = maxBufferSize;
    this.combiner = combiner;
  }

  @Override
  public SourceShape<T> shape() {
    return shape;
  }

  @Override
  public Attributes initialAttributes() {
    return DEFAULT_ATTRIBUTES;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws IOException {
    if (!Files.exists(directoryPath))
      throw new IllegalArgumentException("The path: '" + directoryPath + "' does not exist");
    if (!Files.isDirectory(directoryPath))
      throw new IllegalArgumentException("The path '" + directoryPath + "' is not a directory");

    return new TimerGraphStageLogic(shape) {
      private final Queue<T> buffer = new ArrayDeque<>();
      private final WatchService service = directoryPath.getFileSystem().newWatchService();
      private final WatchKey watchKey =
          directoryPath.register(
              service,
              new WatchEvent.Kind<?>[] {ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE, OVERFLOW},
              // this is com.sun internal, but the service is useless on OSX without it
              SensitivityWatchEventModifier.HIGH);

      {
        setHandler(
            out,
            new AbstractOutHandler() {

              @Override
              public void onPull() throws Exception {
                if (!buffer.isEmpty()) {
                  pushHead();
                } else {
                  doPoll();
                  if (!buffer.isEmpty()) {
                    pushHead();
                  } else {
                    schedulePoll();
                  }
                }
              }
            });
      }

      @Override
      public void onTimer(Object timerKey) {
        if (!isClosed(out)) {
          doPoll();
          if (!buffer.isEmpty()) {
            pushHead();
          } else {
            schedulePoll();
          }
        }
      }

      @Override
      public void postStop() {
        try {
          if (watchKey.isValid()) watchKey.cancel();
          service.close();
        } catch (Exception ex) {
          // Remove when #21168 is in a release
          throw new RuntimeException(ex);
        }
      }

      private void pushHead() {
        final T head = buffer.poll();
        if (head != null) {
          push(out, head);
        }
      }

      private void schedulePoll() {
        scheduleOnce("poll", pollInterval);
      }

      private void doPoll() {
        try {
          for (WatchEvent<?> event : watchKey.pollEvents()) {
            final WatchEvent.Kind<?> kind = event.kind();

            if (OVERFLOW.equals(kind)) {
              // overflow means that some file system change events may have been missed,
              // that may be ok for some scenarios but to make sure it does not pass unnoticed we
              // fail the stage
              failStage(
                  new RuntimeException("Overflow from watch service: '" + directoryPath + "'"));

            } else {
              // if it's not an overflow it must be a Path event
              @SuppressWarnings("unchecked")
              final Path path = (Path) event.context();
              final Path absolutePath = directoryPath.resolve(path);
              final DirectoryChange change = kindToChange(kind);

              buffer.add(combiner.apply(absolutePath, change));
              if (buffer.size() > maxBufferSize) {
                failStage(
                    new RuntimeException(
                        "Max event buffer size " + maxBufferSize + " reached for $path"));
              }
            }
          }
        } finally {
          if (!watchKey.reset()) {
            // directory no longer accessible
            completeStage();
          }
        }
      }

      // convert from the parametrized API to our much nicer API enum
      private DirectoryChange kindToChange(WatchEvent.Kind<?> kind) {
        final DirectoryChange change;
        if (kind.equals(ENTRY_CREATE)) {
          change = DirectoryChange.Creation;
        } else if (kind.equals(ENTRY_DELETE)) {
          change = DirectoryChange.Deletion;
        } else if (kind.equals(ENTRY_MODIFY)) {
          change = DirectoryChange.Modification;
        } else {
          throw new RuntimeException(
              "Unexpected kind of event gotten from watch service for path '"
                  + directoryPath
                  + "': "
                  + kind);
        }
        return change;
      }
    };
  }

  @Override
  public String toString() {
    return "DirectoryChangesSource(" + directoryPath + ')';
  }

  // factory methods

  /**
   * Java API
   *
   * @param directoryPath Directory to watch
   * @param pollInterval Interval between polls to the JDK watch service when a push comes in and
   *     there was no changes, if the JDK implementation is slow, it will not help lowering this
   * @param maxBufferSize Maximum number of buffered directory changes before the stage fails
   */
  @SuppressWarnings("unchecked")
  public static Source<Pair<Path, DirectoryChange>, NotUsed> create(
      Path directoryPath, FiniteDuration pollInterval, int maxBufferSize) {
    return Source.fromGraph(
        new DirectoryChangesSource(directoryPath, pollInterval, maxBufferSize, Pair::apply));
  }
}
