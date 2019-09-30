/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.file.impl;

import akka.annotation.InternalApi;
import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.*;
import akka.util.ByteString;
import scala.concurrent.duration.FiniteDuration;
import scala.util.Failure;
import scala.util.Success;
import scala.util.Try;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * INTERNAL API
 *
 * <p>Read the entire contents of a file, and then when the end is reached, keep reading newly
 * appended data. Like the unix command `tail -f`.
 *
 * <p>Aborting the stage can be done by combining with a [[akka.stream.KillSwitch]]
 *
 * <p>To use the stage from Scala see the factory methods in {@link
 * akka.stream.alpakka.file.scaladsl.FileTailSource}
 */
@InternalApi
public final class FileTailSource extends GraphStage<SourceShape<ByteString>> {

  private final Path path;
  private final int maxChunkSize;
  private final long startingPosition;
  private final FiniteDuration pollingInterval;
  private final String pollTimer = "poll";
  private final String fileExistsCheckTimer = "fileCheck";
  private final Outlet<ByteString> out = Outlet.create("FileTailSource.out");
  private final SourceShape<ByteString> shape = SourceShape.of(out);

  // this is stateless, so can be shared among instances
  private static final CompletionHandler<Integer, AsyncCallback<Try<Integer>>> completionHandler =
      new CompletionHandler<Integer, AsyncCallback<Try<Integer>>>() {
        @Override
        public void completed(Integer result, AsyncCallback<Try<Integer>> attachment) {
          attachment.invoke(new Success<>(result));
        }

        @Override
        public void failed(Throwable exc, AsyncCallback<Try<Integer>> attachment) {
          attachment.invoke(new Failure<>(exc));
        }
      };

  public FileTailSource(
      Path path, int maxChunkSize, long startingPosition, FiniteDuration pollingInterval) {
    this.path = path;
    this.maxChunkSize = maxChunkSize;
    this.startingPosition = startingPosition;
    this.pollingInterval = pollingInterval;
  }

  @Override
  public SourceShape<ByteString> shape() {
    return shape;
  }

  @Override
  public GraphStageLogic createLogic(Attributes inheritedAttributes) throws IOException {
    if (!Files.exists(path))
      throw new IllegalArgumentException("Path '" + path + "' does not exist");
    if (Files.isDirectory(path))
      throw new IllegalArgumentException("Path '" + path + "' cannot be tailed, it is a directory");
    if (!Files.isReadable(path))
      throw new IllegalArgumentException("No read permission for '" + path + "'");

    return new TimerGraphStageLogic(shape) {
      private final ByteBuffer buffer = ByteBuffer.allocate(maxChunkSize);
      private final AsynchronousFileChannel channel =
          AsynchronousFileChannel.open(path, StandardOpenOption.READ);

      private long position = startingPosition;
      private AsyncCallback<Try<Integer>> chunkCallback;

      {
        setHandler(
            out,
            new AbstractOutHandler() {
              @Override
              public void onPull() throws Exception {
                doPull();
              }
            });
      }

      @Override
      public void preStart() {
        chunkCallback =
            createAsyncCallback(
                (tryInteger) -> {
                  if (tryInteger.isSuccess()) {
                    int readBytes = tryInteger.get();
                    // when the number of bytes read is -1 it signals that no new data is available
                    if (readBytes > 0) {
                      if (isTimerActive(fileExistsCheckTimer)) {
                        cancelTimer(fileExistsCheckTimer);
                      }
                      buffer.flip();
                      push(out, ByteString.fromByteBuffer(buffer));
                      position += readBytes;
                      buffer.clear();
                    } else {
                      // hit end, try again in a while
                      scheduleOnce(pollTimer, pollingInterval);
                      // check to see if the file still exists
                      scheduleOnce(fileExistsCheckTimer, pollingInterval);
                    }

                  } else {
                    failStage(tryInteger.failed().get());
                  }
                });
      }

      @Override
      public void onTimer(Object timerKey) {
        if (timerKey == pollTimer) {
          doPull();
        } else if (timerKey == fileExistsCheckTimer) {
          fileExistsCheck();
        }
      }

      private void doPull() {
        channel.read(buffer, position, chunkCallback, completionHandler);
      }

      private void fileExistsCheck() {
        if (!Files.exists(path)) {
          this.completeStage();
        }
      }

      @Override
      public void postStop() {
        try {
          if (channel.isOpen()) channel.close();
        } catch (Exception ex) {
          // Remove when #21168 is fixed
          throw new RuntimeException(ex);
        }
      }
    };
  }
}
