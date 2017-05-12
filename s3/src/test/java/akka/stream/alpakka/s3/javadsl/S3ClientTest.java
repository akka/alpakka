/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.javadsl;

import akka.NotUsed;
import akka.http.javadsl.model.Uri;
import akka.http.javadsl.model.headers.ByteRange;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.s3.BufferType;
import akka.stream.alpakka.s3.MemoryBufferType;
import akka.stream.alpakka.s3.Proxy;
import akka.stream.alpakka.s3.S3Settings;
import akka.stream.alpakka.s3.auth.AWSCredentials;
import akka.stream.alpakka.s3.auth.BasicCredentials;
import akka.stream.alpakka.s3.scaladsl.S3WireMockBase;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.Test;
import scala.Some;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class S3ClientTest extends S3WireMockBase {

    final Materializer materializer = ActorMaterializer.create(system());

    //#client
    final AWSCredentials credentials = new BasicCredentials("my-AWS-access-key-ID", "my-AWS-password");
    final Proxy proxy = new Proxy("localhost",port(),"http");
    final S3Settings settings = new S3Settings(MemoryBufferType.getInstance(),"", Some.apply(proxy),credentials,"us-east-1",false);
    final S3Client client = new S3Client(settings, system(), materializer);
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
    public void download() throws Exception {

        mockDownload();

        //#download
        final Source<ByteString, NotUsed> source = client.download(bucket(), bucketKey());
        //#download

        final CompletionStage<String> resultCompletionStage =
                source.map(ByteString::utf8String).runWith(Sink.head(), materializer);

        String result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertEquals(body(), result);
    }

    @Test
    public void rangedDownload() throws Exception {

        mockRangedDownload();

        //#rangedDownload
        final Source<ByteString, NotUsed> source = client.download(bucket(), bucketKey(),
                ByteRange.createSlice(bytesRangeStart(), bytesRangeEnd()));
        //#rangedDownload

        final CompletionStage<byte[]> resultCompletionStage =
                source.map(ByteString::toArray).runWith(Sink.head(), materializer);

        byte[] result = resultCompletionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertTrue(Arrays.equals(rangeOfBody(), result));
    }
}
