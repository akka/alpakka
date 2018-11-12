/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.javadsl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorSystem;
import akka.stream.alpakka.s3.S3Client;
import org.junit.Test;
import akka.NotUsed;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.model.headers.ByteRange;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.s3.MemoryBufferType;
import akka.stream.alpakka.s3.Proxy;
import akka.stream.alpakka.s3.S3Settings;
import akka.stream.alpakka.s3.impl.ListBucketVersion2;
import akka.stream.alpakka.s3.impl.S3Headers;
import akka.stream.alpakka.s3.impl.ServerSideEncryption;
import akka.stream.alpakka.s3.scaladsl.S3WireMockBase;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.AwsRegionProvider;
import scala.Option;
import scala.Some;

public class S3ClientTest extends S3WireMockBase {

  private final ActorSystem system = system();
  private final Materializer materializer = ActorMaterializer.create(system());

  @Test
  public void multipartUpload() throws Exception {

    mockUpload();

    // #upload
    final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink =
        S3.multipartUpload(bucket(), bucketKey(), system);
    // #upload

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        Source.single(ByteString.fromString(body())).runWith(sink, materializer);

    MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        new MultipartUploadResult(
            Uri.create(url()), bucket(), bucketKey(), etag(), Optional.empty()),
        result);
  }

  @Test
  public void multipartUploadSSE() throws Exception {

    mockUploadSSE();

    // #upload
    final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink =
        S3.multipartUpload(bucket(), bucketKey(), sseCustomerKeys(), system);
    // #upload

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        Source.single(ByteString.fromString(body())).runWith(sink, materializer);

    MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        new MultipartUploadResult(
            Uri.create(url()), bucket(), bucketKey(), etag(), Optional.empty()),
        result);
  }

  @Test
  public void download() throws Exception {

    mockDownload();

    // #download
    final CompletionStage<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>>
        sourceAndMeta = S3.download(bucket(), bucketKey(), system);
    final Source<ByteString, NotUsed> source =
        sourceAndMeta.toCompletableFuture().get(5, TimeUnit.SECONDS).get().first();
    // #download

    final CompletionStage<String> resultCompletionStage =
        source.map(ByteString::utf8String).runWith(Sink.head(), materializer);

    String result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(body(), result);
  }

  @Test
  public void head() throws Exception {

    long contentLength = 8L;
    mockHead(contentLength);

    // #objectMetadata
    final CompletionStage<Optional<ObjectMetadata>> source =
        S3.getObjectMetadata(bucket(), bucketKey(), system);
    // #objectMetadata

    Optional<ObjectMetadata> result = source.toCompletableFuture().get(5, TimeUnit.SECONDS);

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

    // #objectMetadata
    final CompletionStage<Optional<ObjectMetadata>> source =
        S3.getObjectMetadata(bucket(), bucketKey(), Optional.of(versionId), null, system);
    // #objectMetadata

    Optional<ObjectMetadata> result = source.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final ObjectMetadata objectMetadata = result.get();
    Optional<String> s3eTag = objectMetadata.getETag();
    Optional<String> metadataVersionId = objectMetadata.getVersionId();

    assertEquals(s3eTag, Optional.of(etag()));
    assertEquals(metadataVersionId, Optional.of(versionId));
  }

  @Test
  public void headServerSideEncryption() throws Exception {
    mockHeadSSEC();

    // #objectMetadata
    final CompletionStage<Optional<ObjectMetadata>> source =
        S3.getObjectMetadata(bucket(), bucketKey(), sseCustomerKeys(), system);
    // #objectMetadata

    Optional<ObjectMetadata> result = source.toCompletableFuture().get(5, TimeUnit.SECONDS);

    Optional<String> etag = result.get().getETag();

    assertEquals(etag, Optional.of(etagSSE()));
  }

  @Test
  public void downloadServerSideEncryption() throws Exception {
    mockDownloadSSEC();

    // #download
    final CompletionStage<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>>
        sourceAndMeta = S3.download(bucket(), bucketKey(), sseCustomerKeys(), system);
    // #download

    final Source<ByteString, NotUsed> source =
        sourceAndMeta.toCompletableFuture().get(5, TimeUnit.SECONDS).get().first();
    final CompletionStage<String> resultCompletionStage =
        source.map(ByteString::utf8String).runWith(Sink.head(), materializer);

    String result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(bodySSE(), result);
  }

  @Test
  public void downloadServerSideEncryptionWithVersion() throws Exception {
    String versionId = "3/L4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo";
    mockDownloadSSECWithVersion(versionId);

    // #download
    final CompletionStage<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>>
        sourceAndMeta =
            S3.download(
                bucket(), bucketKey(), null, Optional.of(versionId), sseCustomerKeys(), system);
    // #download

    final Source<ByteString, NotUsed> source =
        sourceAndMeta.toCompletableFuture().get(5, TimeUnit.SECONDS).get().first();
    final CompletionStage<String> resultCompletionStage =
        source.map(ByteString::utf8String).runWith(Sink.head(), materializer);
    final ObjectMetadata metadata =
        sourceAndMeta.toCompletableFuture().get(5, TimeUnit.SECONDS).get().second();
    final String result = resultCompletionStage.toCompletableFuture().get();

    assertEquals(bodySSE(), result);
    assertEquals(Optional.of(versionId), metadata.getVersionId());
  }

  @Test
  public void rangedDownload() throws Exception {

    mockRangedDownload();

    // #rangedDownload
    final CompletionStage<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>>
        sourceAndMeta =
            S3.download(
                bucket(),
                bucketKey(),
                ByteRange.createSlice(bytesRangeStart(), bytesRangeEnd()),
                system);
    // #rangedDownload

    final Source<ByteString, NotUsed> source =
        sourceAndMeta.toCompletableFuture().get(5, TimeUnit.SECONDS).get().first();
    final CompletionStage<byte[]> resultCompletionStage =
        source.map(ByteString::toArray).runWith(Sink.head(), materializer);

    byte[] result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertTrue(Arrays.equals(rangeOfBody(), result));
  }

  @Test
  public void rangedDownloadServerSideEncryption() throws Exception {

    mockRangedDownloadSSE();

    // #rangedDownload
    final CompletionStage<Optional<Pair<Source<ByteString, NotUsed>, ObjectMetadata>>>
        sourceAndMeta =
            S3.download(
                bucket(),
                bucketKey(),
                ByteRange.createSlice(bytesRangeStart(), bytesRangeEnd()),
                sseCustomerKeys(),
                system);
    // #rangedDownload

    final Source<ByteString, NotUsed> source =
        sourceAndMeta.toCompletableFuture().get(5, TimeUnit.SECONDS).get().first();
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
        S3.listBucket(bucket(), Option.apply(listPrefix()), system);
    // #list-bucket

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
        S3.multipartCopy(bucket, sourceKey, targetBucket, targetKey, system);
    // #multipart-copy

    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        new MultipartUploadResult(
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
            S3Headers.empty(),
            null,
            system // encryption
            );
    // #multipart-copy-with-source-version

    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        new MultipartUploadResult(
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
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey(), system);
    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        new MultipartUploadResult(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void copyUploadWithContentLengthGreaterThenChunkSize() throws Exception {
    mockCopyMulti();

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey(), system);
    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        new MultipartUploadResult(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }

  @Test
  public void copyUploadEmptyFile() throws Exception {
    mockCopy(0);

    final CompletionStage<MultipartUploadResult> resultCompletionStage =
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey(), system);
    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        new MultipartUploadResult(
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
            S3Headers.empty(),
            ServerSideEncryption.AES256$.MODULE$,
            system);

    final MultipartUploadResult result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        result,
        new MultipartUploadResult(
            Uri.create(targetUrl()), targetBucket(), targetBucketKey(), etag(), Optional.empty()));
  }
}
