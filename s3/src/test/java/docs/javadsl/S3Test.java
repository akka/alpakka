/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import akka.Done;
import akka.actor.ActorSystem;
import akka.http.javadsl.model.*;
import akka.stream.Attributes;
import akka.stream.alpakka.s3.*;
import akka.stream.alpakka.s3.headers.CustomerKeys;
import akka.stream.alpakka.s3.headers.ServerSideEncryption;
import akka.stream.alpakka.s3.javadsl.S3;
import akka.stream.javadsl.Sink$;
import org.junit.Test;
import akka.NotUsed;
import akka.http.javadsl.model.headers.ByteRange;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.s3.scaladsl.S3WireMockBase;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import scala.Option;
import scala.collection.immutable.List;

public class S3Test extends S3WireMockBase {

  private final Materializer materializer = ActorMaterializer.create(system());

  private final S3Settings sampleSettings = S3Ext.get(system()).settings();

  @Test
  public void multipartUpload() throws Exception {

    mockUpload();

    // #upload
    final Source<ByteString, NotUsed> file = Source.single(ByteString.fromString(body()));

    final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink =
        S3.multipartUpload(bucket(), bucketKey());

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        file.runWith(sink, materializer);
    // #upload

    MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        MultipartUploadResult.create(
            Uri.create(url()), bucket(), bucketKey(), etag(), Optional.empty()),
        result);
  }

  @Test
  public void multipartUploadSSE() throws Exception {

    mockUploadSSE();

    final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink =
        S3.multipartUpload(
            bucket(),
            bucketKey(),
            ContentTypes.APPLICATION_OCTET_STREAM,
            S3Headers.create().withServerSideEncryption(sseCustomerKeys()));

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        Source.single(ByteString.fromString(body())).runWith(sink, materializer);

    MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        MultipartUploadResult.create(
            Uri.create(url()), bucket(), bucketKey(), etag(), Optional.empty()),
        result);
  }

  @Test
  public void download() throws Exception {

    mockDownload();

    // #download
    final Source<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>, NotUsed>
        sourceAndMeta = S3.download(bucket(), bucketKey());
    final Pair<Source<ByteString, NotUsed>, ObjectMetadata> dataAndMetadata =
        sourceAndMeta
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get();

    final Source<ByteString, NotUsed> data = dataAndMetadata.first();
    final ObjectMetadata metadata = dataAndMetadata.second();

    final CompletionStage<String> resultCompletionStage =
        data.map(ByteString::utf8String).runWith(Sink.head(), materializer);

    String result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
    // #download

    assertEquals(body(), result);

    System.out.println("#####");
    System.out.println(metadata.getContentLength());

    // #downloadToAkkaHttp
    HttpResponse.create()
        .withEntity(
            HttpEntities.create(
                metadata
                    .getContentType()
                    .map(ct -> ContentTypes.parse(ct))
                    .orElse(ContentTypes.APPLICATION_OCTET_STREAM),
                metadata.getContentLength(),
                data));
    // #downloadToAkkaHttp
  }

  @Test
  public void head() throws Exception {

    long contentLength = 8L;
    mockHead(contentLength);

    // #objectMetadata
    final Source<Optional<ObjectMetadata>, NotUsed> source =
        S3.getObjectMetadata(bucket(), bucketKey());
    // #objectMetadata

    Optional<ObjectMetadata> result =
        source.runWith(Sink.head(), materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    final ObjectMetadata objectMetadata = result.get();
    Optional<String> s3eTag = objectMetadata.getETag();
    long actualContentLength = objectMetadata.getContentLength();
    Optional<String> versionId = objectMetadata.getVersionId();

    assertEquals(s3eTag, Optional.of(etag()));
    assertEquals(actualContentLength, contentLength);
    assertEquals(versionId, Optional.empty());
  }

  @Test
  public void headWithVersion() throws Exception {
    String versionId = "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo";
    mockHeadWithVersion(versionId);

    final Source<Optional<ObjectMetadata>, NotUsed> source =
        S3.getObjectMetadata(bucket(), bucketKey(), Optional.of(versionId), null);

    Optional<ObjectMetadata> result =
        source.runWith(Sink.head(), materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    final ObjectMetadata objectMetadata = result.get();
    Optional<String> s3eTag = objectMetadata.getETag();
    Optional<String> metadataVersionId = objectMetadata.getVersionId();

    assertEquals(s3eTag, Optional.of(etag()));
    assertEquals(metadataVersionId, Optional.of(versionId));
  }

  @Test
  public void headServerSideEncryption() throws Exception {
    mockHeadSSEC();

    final Source<Optional<ObjectMetadata>, NotUsed> source =
        S3.getObjectMetadata(bucket(), bucketKey(), sseCustomerKeys());

    Optional<ObjectMetadata> result =
        source.runWith(Sink.head(), materializer).toCompletableFuture().get(5, TimeUnit.SECONDS);

    Optional<String> etag = result.get().getETag();

    assertEquals(etag, Optional.of(etagSSE()));
  }

  @Test
  public void downloadServerSideEncryption() throws Exception {
    mockDownloadSSEC();

    final Source<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>, NotUsed>
        sourceAndMeta = S3.download(bucket(), bucketKey(), sseCustomerKeys());

    final Source<ByteString, NotUsed> source =
        sourceAndMeta
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .first();
    final CompletionStage<String> resultCompletionStage =
        source.map(ByteString::utf8String).runWith(Sink.head(), materializer);

    String result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(bodySSE(), result);
  }

  @Test
  public void downloadServerSideEncryptionWithVersion() throws Exception {
    String versionId = "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo";
    mockDownloadSSECWithVersion(versionId);

    final Source<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>, NotUsed>
        sourceAndMeta =
            S3.download(bucket(), bucketKey(), null, Optional.of(versionId), sseCustomerKeys());

    final Source<ByteString, NotUsed> source =
        sourceAndMeta
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .first();
    final CompletionStage<String> resultCompletionStage =
        source.map(ByteString::utf8String).runWith(Sink.head(), materializer);
    final ObjectMetadata metadata =
        sourceAndMeta
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .second();
    final String result = resultCompletionStage.toCompletableFuture().get();

    assertEquals(bodySSE(), result);
    assertEquals(Optional.of(versionId), metadata.getVersionId());
  }

  @Test
  public void rangedDownload() throws Exception {

    mockRangedDownload();

    // #rangedDownload
    final Source<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>, NotUsed>
        sourceAndMeta =
            S3.download(
                bucket(), bucketKey(), ByteRange.createSlice(bytesRangeStart(), bytesRangeEnd()));
    // #rangedDownload

    final Source<ByteString, NotUsed> source =
        sourceAndMeta
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .first();
    final CompletionStage<byte[]> resultCompletionStage =
        source.map(ByteString::toArray).runWith(Sink.head(), materializer);

    byte[] result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertTrue(Arrays.equals(rangeOfBody(), result));
  }

  @Test
  public void rangedDownloadServerSideEncryption() throws Exception {

    mockRangedDownloadSSE();

    final Source<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>, NotUsed>
        sourceAndMeta =
            S3.download(
                bucket(),
                bucketKey(),
                ByteRange.createSlice(bytesRangeStart(), bytesRangeEnd()),
                sseCustomerKeys());

    final Source<ByteString, NotUsed> source =
        sourceAndMeta
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .first();
    final CompletionStage<byte[]> resultCompletionStage =
        source.map(ByteString::toArray).runWith(Sink.head(), materializer);

    byte[] result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertTrue(Arrays.equals(rangeOfBodySSE(), result));
  }

  @Test
  public void listBucket() throws Exception {

    mockListBucket();

    // #list-bucket
    final Source<ListBucketResultContents, NotUsed> keySource =
        S3.listBucket(bucket(), Option.apply(listPrefix()));
    // #list-bucket

    final CompletionStage<ListBucketResultContents> resultCompletionStage =
        keySource.runWith(Sink.head(), materializer);

    ListBucketResultContents result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(result.key(), listKey());
  }

  @Test
  public void listBucketVersion1() throws Exception {
    mockListBucketVersion1();

    // #list-bucket-attributes
    final S3Settings useVersion1Api =
        S3Ext.get(system()).settings().withListBucketApiVersion(ApiVersion.getListBucketVersion1());

    final Source<ListBucketResultContents, NotUsed> keySource =
        S3.listBucket(bucket(), Option.apply(listPrefix()))
            .withAttributes(S3Attributes.settings(useVersion1Api));
    // #list-bucket-attributes

    final CompletionStage<ListBucketResultContents> resultCompletionStage =
        keySource.runWith(Sink.head(), materializer);

    ListBucketResultContents result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(result.key(), listKey());
  }

  @Test
  public void copyUploadWithContentLengthLessThenChunkSize() throws Exception {
    mockCopy();

    String bucket = bucket();
    String sourceKey = bucketKey();
    String targetBucket = targetBucket();
    String targetKey = targetBucketKey();
    // #multipart-copy
    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(bucket, sourceKey, targetBucket, targetKey).run(materializer);
    // #multipart-copy

    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        MultipartUploadResult.create(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void copyUploadWithSourceVersion() throws Exception {
    mockCopyVersioned();

    String bucket = bucket();
    String sourceKey = bucketKey();
    String targetBucket = targetBucket();
    String targetKey = targetBucketKey();

    // #multipart-copy-with-source-version
    String sourceVersionId = "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo";
    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(
                bucket,
                sourceKey,
                targetBucket,
                targetKey,
                Optional.of(sourceVersionId),
                S3Headers.create())
            .run(materializer);
    // #multipart-copy-with-source-version

    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        MultipartUploadResult.create(
            Uri.create(targetUrl()),
            targetBucket(),
            targetBucketKey(),
            etag(),
            Optional.of("43jfkodU8493jnFJD9fjj3HHNVfdsQUIFDNsidf038jfdsjGFDSIRp")));
  }

  @Test
  public void copyUploadWithContentLengthEqualToChunkSize() throws Exception {
    mockCopy(5242880);

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey())
            .run(materializer);
    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        MultipartUploadResult.create(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void copyUploadWithContentLengthGreaterThenChunkSize() throws Exception {
    mockCopyMulti();

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey())
            .run(materializer);
    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        MultipartUploadResult.create(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void copyUploadEmptyFile() throws Exception {
    mockCopy(0);

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey())
            .run(materializer);
    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        MultipartUploadResult.create(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void copyUploadWithSSE() throws Exception {
    mockCopySSE();

    // #multipart-copy-sse
    final CustomerKeys keys =
        ServerSideEncryption.customerKeys(sseCustomerKey()).withMd5(sseCustomerMd5Key());

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(
                bucket(),
                bucketKey(),
                targetBucket(),
                targetBucketKey(),
                S3Headers.create().withServerSideEncryption(keys))
            .run(materializer);
    // #multipart-copy-sse

    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        MultipartUploadResult.create(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void copyUploadWithCustomHeader() throws Exception {
    mockCopy();

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(
                bucket(),
                bucketKey(),
                targetBucket(),
                targetBucketKey(),
                S3Headers.create().withServerSideEncryption(ServerSideEncryption.aes256()))
            .run(materializer);

    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        MultipartUploadResult.create(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void makeBucket() throws Exception {
    mockMakingBucket();

    // #make-bucket
    final Attributes sampleAttributes = S3Attributes.settings(sampleSettings);

    final String bucketName = "samplebucket1";

    CompletionStage<Done> makeBucketRequest = S3.makeBucket(bucketName, materializer);
    CompletionStage<Done> makeBucketRequestWithAttributes =
        S3.makeBucket(bucketName, materializer, sampleAttributes);
    Source<Done, NotUsed> makeBucketSourceRequest = S3.makeBucketSource(bucketName);
    // #make-bucket

    assertEquals(makeBucketRequest.toCompletableFuture().get(5, TimeUnit.SECONDS), Done.done());
    assertEquals(
        makeBucketRequestWithAttributes.toCompletableFuture().get(5, TimeUnit.SECONDS),
        Done.done());
    assertEquals(
        makeBucketSourceRequest
            .runWith(Sink.ignore(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        Done.done());
  }

  @Test
  public void deleteBucket() throws Exception {
    final String bucketName = "samplebucket1";

    mockDeletingBucket();

    // #delete-bucket
    final Attributes sampleAttributes = S3Attributes.settings(sampleSettings);

    CompletionStage<Done> deleteBucketRequest = S3.deleteBucket(bucketName, materializer);
    CompletionStage<Done> deleteBucketRequestWithAttribues =
        S3.deleteBucket(bucketName, materializer, sampleAttributes);

    Source<Done, NotUsed> deleteBucketSourceRequest = S3.deleteBucketSource(bucketName);
    // #delete-bucket

    assertEquals(deleteBucketRequest.toCompletableFuture().get(5, TimeUnit.SECONDS), Done.done());

    assertEquals(
        deleteBucketRequestWithAttribues.toCompletableFuture().get(5, TimeUnit.SECONDS),
        Done.done());

    assertEquals(
        deleteBucketSourceRequest
            .runWith(Sink.ignore(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        Done.done());
  }

  @Test
  public void checkIfBucketExistsForNonExisting() throws Exception {
    mockCheckingBucketStateForNonExistingBucket();

    // #check-if-bucket-exists
    final Attributes sampleAttributes = S3Attributes.settings(sampleSettings);

    final CompletionStage<BucketAccess> doesntExistRequest =
        S3.checkIfBucketExists(bucket(), materializer);
    final CompletionStage<BucketAccess> doesntExistRequestWithAttributes =
        S3.checkIfBucketExists(bucket(), materializer, sampleAttributes);

    final Source<BucketAccess, NotUsed> doesntExistSourceRequest =
        S3.checkIfBucketExistsSource(bucket());
    // #check-if-bucket-exists

    assertEquals(
        doesntExistRequest.toCompletableFuture().get(5, TimeUnit.SECONDS),
        BucketAccess.notExists());

    assertEquals(
        doesntExistRequestWithAttributes.toCompletableFuture().get(5, TimeUnit.SECONDS),
        BucketAccess.notExists());

    assertEquals(
        doesntExistSourceRequest
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        BucketAccess.notExists());
  }

  @Test
  public void checkIfBucketExistsForExisting() throws Exception {
    mockCheckingBucketStateForExistingBucket();

    final CompletionStage<BucketAccess> existRequest =
        S3.checkIfBucketExists(bucket(), materializer);

    final Source<BucketAccess, NotUsed> existSourceRequest = S3.checkIfBucketExistsSource(bucket());

    assertEquals(
        existRequest.toCompletableFuture().get(5, TimeUnit.SECONDS), BucketAccess.accessGranted());

    assertEquals(
        existSourceRequest
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        BucketAccess.accessGranted());
  }

  @Test
  public void checkIfBucketExistsForBucketWithoutRights() throws Exception {
    mockCheckingBucketStateForBucketWithoutRights();

    final CompletionStage<BucketAccess> noRightsRequest =
        S3.checkIfBucketExists(bucket(), materializer);

    final Source<BucketAccess, NotUsed> noRightsSourceRequest =
        S3.checkIfBucketExistsSource(bucket());

    assertEquals(
        noRightsRequest.toCompletableFuture().get(5, TimeUnit.SECONDS),
        BucketAccess.accessDenied());

    assertEquals(
        noRightsSourceRequest
            .runWith(Sink.head(), materializer)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        BucketAccess.accessDenied());
  }
}
