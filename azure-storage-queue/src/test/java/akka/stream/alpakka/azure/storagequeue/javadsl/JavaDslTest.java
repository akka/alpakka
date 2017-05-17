/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.azure.storagequeue.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.alpakka.azure.storagequeue.*;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.JavaTestKit;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.queue.*;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

public class JavaDslTest extends JUnitSuite {
  private static ActorSystem system;
  private static ActorMaterializer materializer;
  private static final String storageConnectionString = System.getenv("AZURE_CONNECTION_STRING");
  private static final Supplier<CloudQueue> queueSupplier = new Supplier<CloudQueue>() {
      public CloudQueue get() {
        try {
          if (storageConnectionString == null) {
            return null;
          }
          CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
          CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
          return queueClient.getQueueReference("testqueue");       
        }
        catch (Exception ex) {
          throw new RuntimeException("Could not create CloudQueue", ex);
        }
      }
    };

  private static final CloudQueue queue = queueSupplier.get();

  @BeforeClass
  public static void setup()
    throws StorageException, java.net.URISyntaxException, java.security.InvalidKeyException {
    system = ActorSystem.create();
    materializer = ActorMaterializer.create(system);

    if (queue != null) {
      queue.createIfNotExists();
    }
  }

  @AfterClass
  public static void teardown() throws StorageException {
    JavaTestKit.shutdownActorSystem(system);
    if (queue != null) {
      queue.deleteIfExists();
    }
  }

  @Before
  public void clearQueue() throws StorageException {
    if (queue != null) {
      queue.clear();
    }
  }

  @Test
  public void testAzureQueueSink()
    throws StorageException, InterruptedException, ExecutionException, TimeoutException {
    Assume.assumeNotNull(queue);
    final Source<Integer, NotUsed> sourceInt = Source.range(1, 10);
    final Source<CloudQueueMessage, NotUsed> source =
      sourceInt.map(i -> new CloudQueueMessage("Java Azure Cloud Test " + i.toString()));

    final Sink<CloudQueueMessage, CompletionStage<Done>> sink = AzureQueueSink.create(queueSupplier);

    source.runWith(sink, materializer).toCompletableFuture().get(10, TimeUnit.SECONDS);

    Assert.assertNotNull(queue.retrieveMessage());
  }

  @Test
  public void testAzureQueueWithTimeoutsSink()
    throws StorageException, InterruptedException, ExecutionException, TimeoutException {
    Assume.assumeNotNull(queue);
    final Source<Integer, NotUsed> sourceInt = Source.range(1, 10);
    final Source<MessageWithTimeouts, NotUsed> source =
      sourceInt.map(
                    i ->
                    new MessageWithTimeouts(
                                            new CloudQueueMessage("Java Azure Cloud Test " + i.toString()), 0, 600));

    final Sink<MessageWithTimeouts, CompletionStage<Done>> sink =
      AzureQueueWithTimeoutsSink.create(queueSupplier);

    source.runWith(sink, materializer).toCompletableFuture().get(10, TimeUnit.SECONDS);

    Assert.assertNull(
                      queue.retrieveMessage()); // There should be no message because of inital visibility timeout
  }

  @Test
  public void testAzureQueueSource()
    throws StorageException, InterruptedException, ExecutionException, TimeoutException {
    Assume.assumeNotNull(queue);

    // Queue 10 Messages
    for (int i = 0; i < 10; i++) {
      queue.addMessage(new CloudQueueMessage("Java Test " + Integer.toString(i)));
    }

    final Source<CloudQueueMessage, NotUsed> source = AzureQueueSource.create(queueSupplier);

    final CompletionStage<List<CloudQueueMessage>> msgs =
      source.take(10).runWith(Sink.seq(), materializer);

    msgs.toCompletableFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  public void testAzureQueueDeleteSink()
    throws StorageException, InterruptedException, ExecutionException, TimeoutException {
    Assume.assumeNotNull(queue);

    // Queue 10 Messages
    for (int i = 0; i < 10; i++) {
      queue.addMessage(new CloudQueueMessage("Java Test " + Integer.toString(i)));
    }

    // We limit us to buffers of size 1 here, so that there are no stale message in the buffer
    final Source<CloudQueueMessage, NotUsed> source =
      AzureQueueSource.create(queueSupplier, AzureQueueSourceSettings.create(20, 1, 0));

    final Sink<CloudQueueMessage, CompletionStage<Done>> deleteSink =
      AzureQueueDeleteSink.create(queueSupplier);

    final CompletionStage<Done> done = source.take(10).runWith(deleteSink, materializer);

    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    Assert.assertNull(queue.retrieveMessage());
  }

  @Test
  public void testAzureQueueDeleteOrUpdateSink()
    throws StorageException, InterruptedException, ExecutionException, TimeoutException {
    Assume.assumeNotNull(queue);

    // Queue 10 Messages
    for (int i = 0; i < 10; i++) {
      queue.addMessage(new CloudQueueMessage("Java Test " + Integer.toString(i)));
    }

    // We limit us to buffers of size 1 here, so that there are no stale message in the buffer
    final Source<CloudQueueMessage, NotUsed> source =
      AzureQueueSource.create(queueSupplier, AzureQueueSourceSettings.create(20, 1, 0));

    final Sink<MessageAndDeleteOrUpdate, CompletionStage<Done>> deleteOrUpdateSink =
      AzureQueueDeleteOrUpdateSink.create(queueSupplier);

    final CompletionStage<Done> done =
      source
      .take(10)
      .map(msg -> new MessageAndDeleteOrUpdate(msg, MessageAndDeleteOrUpdate.delete()))
      .runWith(deleteOrUpdateSink, materializer);

    done.toCompletableFuture().get(10, TimeUnit.SECONDS);

    Assert.assertNull(queue.retrieveMessage());
  }
}
