/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.http.javadsl.Http;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpEntities;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.model.headers.ByteRange;
import akka.japi.Pair;
import akka.stream.Attributes;
import akka.stream.alpakka.s3.*;
import akka.stream.alpakka.s3.headers.CustomerKeys;
import akka.stream.alpakka.s3.headers.ServerSideEncryption;
import akka.stream.alpakka.s3.javadsl.S3;
import akka.stream.alpakka.s3.scaladsl.S3WireMockBase;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3Test extends S3WireMockBase {
  @Rule public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

  private static ActorSystem system;
  private static WireMockServer wireMockServerForShutdown;

  private final S3Settings sampleSettings = S3Ext.get(system()).settings();
  private final String prefix = listPrefix();
  private final String delimiter = listDelimiter();

  @Before
  public void before() {
    wireMockServerForShutdown = _wireMockServer();
    system = system();
  }

  @AfterClass
  public static void afterAll() throws Exception {
    wireMockServerForShutdown.stop();
    Http.get(system)
        .shutdownAllConnectionPools()
        .thenRun(() -> TestKit.shutdownActorSystem(system));
  }

  @Test
  public void multipartUpload() throws Exception {

    mockUpload();

    // #upload
    final Source<ByteString, NotUsed> file = Source.single(ByteString.fromString(body()));

    final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink =
        S3.multipartUpload(bucket(), bucketKey());

    final CompletionStage<MultipartUploadResult> resultCompletionStage = file.runWith(sink, system);
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
        Source.single(ByteString.fromString(body())).runWith(sink, system);

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
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get();

    final Source<ByteString, NotUsed> data = dataAndMetadata.first();
    final ObjectMetadata metadata = dataAndMetadata.second();

    final CompletionStage<String> resultCompletionStage =
        data.map(ByteString::utf8String).runWith(Sink.head(), system);

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
        source.runWith(Sink.head(), system).toCompletableFuture().get(5, TimeUnit.SECONDS);

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
        source.runWith(Sink.head(), system).toCompletableFuture().get(5, TimeUnit.SECONDS);

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
        source.runWith(Sink.head(), system).toCompletableFuture().get(5, TimeUnit.SECONDS);

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
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .first();
    final CompletionStage<String> resultCompletionStage =
        source.map(ByteString::utf8String).runWith(Sink.head(), system);

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

    final Pair<Source<ByteString, NotUsed>, ObjectMetadata> p =
        sourceAndMeta
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .orElseThrow(() -> new RuntimeException("empty Optional from S3.download"));

    final Source<ByteString, NotUsed> source = p.first();
    final CompletionStage<String> resultCompletionStage =
        source.map(ByteString::utf8String).runWith(Sink.head(), system);
    final String result = resultCompletionStage.toCompletableFuture().get(2, TimeUnit.SECONDS);
    assertEquals(bodySSE(), result);

    final ObjectMetadata metadata = p.second();
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
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .first();
    final CompletionStage<byte[]> resultCompletionStage =
        source.map(ByteString::toArray).runWith(Sink.head(), system);

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
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .get()
            .first();
    final CompletionStage<byte[]> resultCompletionStage =
        source.map(ByteString::toArray).runWith(Sink.head(), system);

    byte[] result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertTrue(Arrays.equals(rangeOfBodySSE(), result));
  }

  @Test
  public void listBucket() throws Exception {

    mockListBucket();

    // #list-bucket
    final Source<ListBucketResultContents, NotUsed> keySource =
        S3.listBucket(bucket(), Optional.of(prefix));
    // #list-bucket

    final CompletionStage<ListBucketResultContents> resultCompletionStage =
        keySource.runWith(Sink.head(), system);

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
        S3.listBucket(bucket(), Optional.of(prefix))
            .withAttributes(S3Attributes.settings(useVersion1Api));
    // #list-bucket-attributes

    final CompletionStage<ListBucketResultContents> resultCompletionStage =
        keySource.runWith(Sink.head(), system);

    ListBucketResultContents result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(result.key(), listKey());
  }

  @Test
  public void listBucketWithDelimiter() throws Exception {

    mockListBucketAndCommonPrefixes();

    // #list-bucket-delimiter
    final Source<ListBucketResultContents, NotUsed> keySource =
        S3.listBucket(bucket(), delimiter, Optional.of(prefix));
    // #list-bucket-delimiter

    final CompletionStage<ListBucketResultContents> resultCompletionStage =
        keySource.runWith(Sink.head(), system);

    ListBucketResultContents result =
        resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(result.key(), listKey());
  }

  @Test
  public void listBucketAndCommonPrefixes() throws Exception {

    mockListBucketAndCommonPrefixes();

    // #list-bucket-and-common-prefixes
    final Source<
            Pair<List<ListBucketResultContents>, List<ListBucketResultCommonPrefixes>>, NotUsed>
        keySource =
            S3.listBucketAndCommonPrefixes(
                bucket(), delimiter, Optional.of(prefix), S3Headers.empty());
    // #list-bucket-and-common-prefixes

    final CompletionStage<
            List<Pair<List<ListBucketResultContents>, List<ListBucketResultCommonPrefixes>>>>
        resultsCompletionStage = keySource.runWith(Sink.seq(), system);

    final List<Pair<List<ListBucketResultContents>, List<ListBucketResultCommonPrefixes>>> results =
        resultsCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final List<ListBucketResultContents> contents = results.get(0).first();
    final List<ListBucketResultCommonPrefixes> commonPrefixes = results.get(0).second();

    assertEquals(contents.get(0).key(), listKey());
    assertEquals(commonPrefixes.get(0).prefix(), listCommonPrefix());
  }

  @Test
  public void listBucketAndCommonPrefixesVersion1() throws Exception {
    mockListBucketAndCommonPrefixesVersion1();

    // #list-bucket-and-common-prefixes-attributes
    final S3Settings useVersion1Api =
        S3Ext.get(system()).settings().withListBucketApiVersion(ApiVersion.getListBucketVersion1());

    final Source<
            Pair<List<ListBucketResultContents>, List<ListBucketResultCommonPrefixes>>, NotUsed>
        keySource =
            S3.listBucketAndCommonPrefixes(
                    bucket(), delimiter, Optional.of(prefix), S3Headers.empty())
                .withAttributes(S3Attributes.settings(useVersion1Api));
    // #list-bucket-and-common-prefixes-attributes

    final CompletionStage<
            List<Pair<List<ListBucketResultContents>, List<ListBucketResultCommonPrefixes>>>>
        resultsCompletionStage = keySource.runWith(Sink.seq(), system);

    final List<Pair<List<ListBucketResultContents>, List<ListBucketResultCommonPrefixes>>> results =
        resultsCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

    final List<ListBucketResultContents> contents = results.get(0).first();
    final List<ListBucketResultCommonPrefixes> commonPrefixes = results.get(0).second();

    assertEquals(contents.get(0).key(), listKey());
    assertEquals(commonPrefixes.get(0).prefix(), listCommonPrefix());
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
        S3.multipartCopy(bucket, sourceKey, targetBucket, targetKey).run(system);
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
            .run(system);
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
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey()).run(system);
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
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey()).run(system);
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
        S3.multipartCopy(bucket(), bucketKey(), targetBucket(), targetBucketKey()).run(system);
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
            .run(system);
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
            .run(system);

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

    CompletionStage<Done> makeBucketRequest = S3.makeBucket(bucketName, system);
    CompletionStage<Done> makeBucketRequestWithAttributes =
        S3.makeBucket(bucketName, system, sampleAttributes);
    Source<Done, NotUsed> makeBucketSourceRequest = S3.makeBucketSource(bucketName);
    // #make-bucket

    assertEquals(makeBucketRequest.toCompletableFuture().get(5, TimeUnit.SECONDS), Done.done());
    assertEquals(
        makeBucketRequestWithAttributes.toCompletableFuture().get(5, TimeUnit.SECONDS),
        Done.done());
    assertEquals(
        makeBucketSourceRequest
            .runWith(Sink.ignore(), system)
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

    CompletionStage<Done> deleteBucketRequest = S3.deleteBucket(bucketName, system);
    CompletionStage<Done> deleteBucketRequestWithAttribues =
        S3.deleteBucket(bucketName, system, sampleAttributes);

    Source<Done, NotUsed> deleteBucketSourceRequest = S3.deleteBucketSource(bucketName);
    // #delete-bucket

    assertEquals(deleteBucketRequest.toCompletableFuture().get(5, TimeUnit.SECONDS), Done.done());

    assertEquals(
        deleteBucketRequestWithAttribues.toCompletableFuture().get(5, TimeUnit.SECONDS),
        Done.done());

    assertEquals(
        deleteBucketSourceRequest
            .runWith(Sink.ignore(), system)
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
        S3.checkIfBucketExists(bucket(), system);
    final CompletionStage<BucketAccess> doesntExistRequestWithAttributes =
        S3.checkIfBucketExists(bucket(), system, sampleAttributes);

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
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        BucketAccess.notExists());
  }

  @Test
  public void checkIfBucketExistsForExisting() throws Exception {
    mockCheckingBucketStateForExistingBucket();

    final CompletionStage<BucketAccess> existRequest = S3.checkIfBucketExists(bucket(), system);

    final Source<BucketAccess, NotUsed> existSourceRequest = S3.checkIfBucketExistsSource(bucket());

    assertEquals(
        existRequest.toCompletableFuture().get(5, TimeUnit.SECONDS), BucketAccess.accessGranted());

    assertEquals(
        existSourceRequest
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        BucketAccess.accessGranted());
  }

  @Test
  public void checkIfBucketExistsForBucketWithoutRights() throws Exception {
    mockCheckingBucketStateForBucketWithoutRights();

    final CompletionStage<BucketAccess> noRightsRequest = S3.checkIfBucketExists(bucket(), system);

    final Source<BucketAccess, NotUsed> noRightsSourceRequest =
        S3.checkIfBucketExistsSource(bucket());

    assertEquals(
        noRightsRequest.toCompletableFuture().get(5, TimeUnit.SECONDS),
        BucketAccess.accessDenied());

    assertEquals(
        noRightsSourceRequest
            .runWith(Sink.head(), system)
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS),
        BucketAccess.accessDenied());
  }
}
