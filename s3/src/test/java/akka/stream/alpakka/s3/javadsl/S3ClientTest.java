/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.javadsl;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import akka.NotUsed;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.model.headers.ByteRange;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.s3.MemoryBufferType;
import akka.stream.alpakka.s3.Proxy;
import akka.stream.alpakka.s3.impl.ListBucketVersion2;
import akka.stream.alpakka.s3.S3Settings;
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
import org.junit.Test;
import scala.Option;
import scala.Some;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3ClientTest extends S3WireMockBase {

    private final Materializer materializer =
            ActorMaterializer.create(system());

    //#client
    private final AWSCredentialsProvider credentials = new AWSStaticCredentialsProvider(
            new BasicAWSCredentials("my-AWS-access-key-ID", "my-AWS-password")
    );

    private AwsRegionProvider regionProvider(String region) {
        return new AwsRegionProvider() {
            @Override
            public String getRegion() throws SdkClientException {
                return region;
            }
        };
    };

    private final Proxy proxy = new Proxy("localhost",port(),"http");
    private final S3Settings settings = new S3Settings(
            MemoryBufferType.getInstance(),
            Some.apply(proxy),
            credentials,
            regionProvider("us-east-1"),
            false,
            scala.Option.empty(),
            ListBucketVersion2.getInstance()
    );
    private final S3Client client = new S3Client(settings, system(), materializer);
    //#client

    @Test
    public void multipartUpload() throws Exception {

        mockUpload();

        //#upload
        final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink = client.multipartUpload(bucket(), bucketKey());
        //#upload

        final CompletionStage<MultipartUploadResult> resultCompletionStage =
                Source.single(ByteString.fromString(body())).runWith(sink, materializer);

        MultipartUploadResult result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(new MultipartUploadResult(Uri.create(url()), bucket(), bucketKey(), etag()), result);
    }

    @Test
    public void multipartUploadSSE() throws Exception {

        mockUploadSSE();

        //#upload
        final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink = client.multipartUpload(bucket(), bucketKey(), sseCustomerKeys());
        //#upload

        final CompletionStage<MultipartUploadResult> resultCompletionStage =
                Source.single(ByteString.fromString(body())).runWith(sink, materializer);

        MultipartUploadResult result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(new MultipartUploadResult(Uri.create(url()), bucket(), bucketKey(), etag()), result);
    }

    @Test
    public void download() throws Exception {

        mockDownload();

        //#download
        final Pair<Source<ByteString, NotUsed>, CompletionStage<ObjectMetadata>> sourceAndMeta = client.download(bucket(), bucketKey());
        final Source<ByteString, NotUsed> source = sourceAndMeta.first();
        //#download

        final CompletionStage<String> resultCompletionStage =
                source.map(ByteString::utf8String).runWith(Sink.head(), materializer);

        String result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(body(), result);
    }

    @Test
    public void head() throws Exception {

        mockHead();

        final CompletionStage<Optional<ObjectMetadata>> source = client.getObjectMetadata(bucket(), bucketKey());

        Optional<ObjectMetadata> result = source.toCompletableFuture().get(5, TimeUnit.SECONDS);

        Optional<String> s3eTag = result.get().getETag();

        assertEquals(s3eTag, Optional.of(etag()));
    }

    @Test
    public void headServerSideEncryption() throws Exception {
        mockHeadSSEC();

        //#objectMetadata
        final CompletionStage<Optional<ObjectMetadata>> source = client.getObjectMetadata(bucket(), bucketKey(), sseCustomerKeys());
        //#objectMetadata

        Optional<ObjectMetadata> result = source.toCompletableFuture().get(5, TimeUnit.SECONDS);

        Optional<String> etag = result.get().getETag();

        assertEquals(etag, Optional.of(etagSSE()));
    }

    @Test
    public void downloadServerSideEncryption() throws Exception {
        mockDownloadSSEC();

        //#download
        final Pair<Source<ByteString, NotUsed>, CompletionStage<ObjectMetadata>> sourceAndMeta = client.download(bucket(), bucketKey(), sseCustomerKeys());
        final Source<ByteString, NotUsed> source = sourceAndMeta.first();
        //#download

        final CompletionStage<String> resultCompletionStage =
                source.map(ByteString::utf8String).runWith(Sink.head(), materializer);

        String result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(bodySSE(), result);
    }

    @Test
    public void rangedDownload() throws Exception {

        mockRangedDownload();

        //#rangedDownload
        final Pair<Source<ByteString, NotUsed>, CompletionStage<ObjectMetadata>> sourceAndMeta = client.download(bucket(), bucketKey(),
                ByteRange.createSlice(bytesRangeStart(), bytesRangeEnd()));
        final Source<ByteString, NotUsed> source = sourceAndMeta.first();
        //#rangedDownload

        final CompletionStage<byte[]> resultCompletionStage =
                source.map(ByteString::toArray).runWith(Sink.head(), materializer);

        byte[] result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertTrue(Arrays.equals(rangeOfBody(), result));
    }

    @Test
    public void rangedDownloadServerSideEncryption() throws Exception {

        mockRangedDownloadSSE();

        //#rangedDownload
        final Pair<Source<ByteString, NotUsed>, CompletionStage<ObjectMetadata>> sourceAndMeta = client.download(bucket(), bucketKey(),
                ByteRange.createSlice(bytesRangeStart(), bytesRangeEnd()), sseCustomerKeys());
        final Source<ByteString, NotUsed> source = sourceAndMeta.first();
        //#rangedDownload

        final CompletionStage<byte[]> resultCompletionStage =
                source.map(ByteString::toArray).runWith(Sink.head(), materializer);

        byte[] result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertTrue(Arrays.equals(rangeOfBodySSE(), result));
    }

    @Test
    public void listBucket() throws Exception {

        mockListBucket();

        //#list-bucket
        final Source<ListBucketResultContents, NotUsed> keySource = client.listBucket(bucket(), Option.apply(listPrefix()));
        //#list-bucket

        final CompletionStage<ListBucketResultContents> resultCompletionStage = keySource.runWith(Sink.head(), materializer);

        ListBucketResultContents result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(result.key(), listKey());

    }
}
