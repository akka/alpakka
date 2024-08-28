/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.stream.alpakka.azure.storage.ObjectMetadata;
import akka.stream.alpakka.azure.storage.javadsl.BlobService;
import akka.stream.alpakka.azure.storage.javadsl.FileService;
import akka.stream.alpakka.azure.storage.scaladsl.StorageWireMockBase;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Sink;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class StorageTest extends StorageWireMockBase {

    @Rule
    public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

    private static ActorSystem system;
    private static WireMockServer wireMockServerForShutdown;

    @Before
    public void before() {
        wireMockServerForShutdown = _wireMockServer();
        system = system();
    }

    @AfterClass
    public static void afterAll() {
        wireMockServerForShutdown.stop();
        Http.get(system)
                .shutdownAllConnectionPools()
                .thenRun(() -> TestKit.shutdownActorSystem(system));
    }


    @Test
    public void createContainer() throws Exception {
        mockCreateContainer();

        //#create-container
        final Source<Optional<ObjectMetadata>, NotUsed> source = BlobService.createContainer(containerName());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#create-container

        final var optionalObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(optionalObjectMetadata.isPresent());
        final var objectMetadata = optionalObjectMetadata.get();
        Assert.assertEquals(objectMetadata.getContentLength(), 0L);
        Assert.assertEquals(objectMetadata.getETag().get(), ETagRawValue());
    }


    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    @Ignore("Fix this test case")
    @Test
    public void putBlob() throws Exception {
        mockPutBlob();

        //#put-blob
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                BlobService.putBlob(containerName() + "/" + blobName(),
                        ContentTypes.TEXT_PLAIN_UTF8,
                        contentLength(),
                        Source.single(ByteString.fromString(payload())),
                         Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#put-blob

        final var optionalObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(optionalObjectMetadata.isPresent());
    }

    @Test
    public void getBlob() throws Exception {
        mockGetBlob();

        //#get-blob
        final Source<ByteString, CompletionStage<ObjectMetadata>> source =
                BlobService.getBlob(containerName() + "/" + blobName(), Optional.empty(), Optional.empty());

        final CompletionStage<List<ByteString>> eventualPayload = source.runWith(Sink.seq(), system);
        //#get-blob

        final var actualPayload = eventualPayload.toCompletableFuture().get().stream()
                .map(ByteString::utf8String).collect(Collectors.joining());
        Assert.assertEquals(actualPayload, payload());
    }

    @Test
    public void getBlobRange() throws Exception {
        mockGetBlobWithRange();

        //#get-blob-range
        final Source<ByteString, CompletionStage<ObjectMetadata>> source =
                BlobService.getBlob(containerName() + "/" + blobName(), subRange(),
                        Optional.empty(), Optional.empty());

        final CompletionStage<List<ByteString>> eventualPayload = source.runWith(Sink.seq(), system);
        //#get-blob-range

        final var actualPayload = eventualPayload.toCompletableFuture().get().stream()
                .map(ByteString::utf8String).collect(Collectors.joining());
        Assert.assertEquals("quick", actualPayload);
    }

    @Test
    public void getBlobProperties() throws Exception {
        mockGetBlobProperties();

        //#get-blob-properties
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                BlobService.getProperties(containerName() + "/" + blobName(), Optional.empty(), Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#get-blob-properties

        final var maybeObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(maybeObjectMetadata.isPresent());
        final var objectMetadata = maybeObjectMetadata.get();
        Assert.assertEquals(Optional.of(ETagRawValue()), objectMetadata.getETag());
        Assert.assertEquals(contentLength(), objectMetadata.getContentLength());
        Assert.assertEquals(Optional.of(ContentTypes.TEXT_PLAIN_UTF8.toString()), objectMetadata.getContentType());
    }

    @Test
    public void deleteBlob() throws Exception {
        mockDeleteBlob();

        //#delete-blob
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                BlobService.deleteBlob(containerName() + "/" + blobName(), Optional.empty(), Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#delete-blob

        final var maybeObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(maybeObjectMetadata.isPresent());
        final var objectMetadata = maybeObjectMetadata.get();
        Assert.assertEquals(Optional.of(ETagRawValue()), objectMetadata.getETag());
        Assert.assertEquals(0L, objectMetadata.getContentLength());
    }

    @Test
    public void createFile() throws Exception {
        mockCreateFile();

        //#create-file
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                FileService.createFile(containerName() + "/" + blobName(), ContentTypes.TEXT_PLAIN_UTF8, contentLength(), Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#create-file

        final var maybeObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(maybeObjectMetadata.isPresent());
        final var objectMetadata = maybeObjectMetadata.get();
        Assert.assertEquals(Optional.of(ETagRawValue()), objectMetadata.getETag());
        Assert.assertEquals(0L, objectMetadata.getContentLength());
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    @Ignore("Fix this test case")
    @Test
    public void updateRange() throws Exception {
        mockCreateFile();

        //#update-range
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                FileService.updateRange(containerName() + "/" + blobName(),
                        ContentTypes.TEXT_PLAIN_UTF8, contentRange(), Source.single(ByteString.fromString(payload())),
                        Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#update-range

        final var maybeObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(maybeObjectMetadata.isPresent());
        final var objectMetadata = maybeObjectMetadata.get();
        Assert.assertEquals(Optional.of(ETagRawValue()), objectMetadata.getETag());
        Assert.assertEquals(0L, objectMetadata.getContentLength());
    }

    @Test
    public void getFile() throws Exception {
        mockGetBlob();

        //#get-file
        final Source<ByteString, CompletionStage<ObjectMetadata>> source =
                FileService.getFile(containerName() + "/" + blobName(), Optional.empty(), Optional.empty());

        final CompletionStage<List<ByteString>> eventualPayload = source.runWith(Sink.seq(), system);
        //#get-file

        final var actualPayload = eventualPayload.toCompletableFuture().get().stream()
                .map(ByteString::utf8String).collect(Collectors.joining());
        Assert.assertEquals(actualPayload, payload());
    }

    @Test
    public void getFileProperties() throws Exception {
        mockGetBlobProperties();

        //#get-file-properties
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                FileService.getProperties(containerName() + "/" + blobName(), Optional.empty(), Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#get-file-properties

        final var maybeObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(maybeObjectMetadata.isPresent());
        final var objectMetadata = maybeObjectMetadata.get();
        Assert.assertEquals(Optional.of(ETagRawValue()), objectMetadata.getETag());
        Assert.assertEquals(contentLength(), objectMetadata.getContentLength());
        Assert.assertEquals(Optional.of(ContentTypes.TEXT_PLAIN_UTF8.toString()), objectMetadata.getContentType());
    }

    // TODO: There are couple of issues, firstly there are two `Content-Length` headers being added, one by `putBlob`
    // function and secondly by, most likely, by WireMock. Need to to figure out how to tell WireMock not to add `Content-Length`
    // header, secondly once that resolve then we get `akka.http.scaladsl.model.EntityStreamException`.
    @Ignore("Fix this test case")
    @Test
    public void clearRange() throws Exception {
        mockClearRange();

        //#clear-range
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                FileService.clearRange(containerName() + "/" + blobName(), subRange(), Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#clear-range

        final var maybeObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(maybeObjectMetadata.isPresent());
        final var objectMetadata = maybeObjectMetadata.get();
        Assert.assertEquals(Optional.of(ETagRawValue()), objectMetadata.getETag());
        Assert.assertEquals(0L, objectMetadata.getContentLength());
    }

    @Test
    public void deleteFile() throws Exception {
        mockDeleteBlob();

        //#delete-file
        final Source<Optional<ObjectMetadata>, NotUsed> source =
                FileService.deleteFile(containerName() + "/" + blobName(), Optional.empty(), Optional.empty());

        final CompletionStage<Optional<ObjectMetadata>> optionalCompletionStage = source.runWith(Sink.head(), system);
        //#delete-file

        final var maybeObjectMetadata = optionalCompletionStage.toCompletableFuture().get();
        Assert.assertTrue(maybeObjectMetadata.isPresent());
        final var objectMetadata = maybeObjectMetadata.get();
        Assert.assertEquals(Optional.of(ETagRawValue()), objectMetadata.getETag());
        Assert.assertEquals(0L, objectMetadata.getContentLength());
    }
}
