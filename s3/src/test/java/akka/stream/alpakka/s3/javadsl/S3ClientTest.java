/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.s3.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;

import akka.http.javadsl.model.Uri;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.s3.auth.AWSCredentials;
import akka.stream.alpakka.s3.auth.BasicCredentials;
import akka.stream.alpakka.s3.scaladsl.S3WireMockBase;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import org.junit.Test;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class S3ClientTest extends S3WireMockBase {

    final Materializer materializer = ActorMaterializer.create(system());

    //#client
    final AWSCredentials credentials = new BasicCredentials("my-AWS-access-key-ID", "my-AWS-password");
    final S3Client client = new S3Client(credentials, "us-east-1", system(), materializer);
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
    public void multipartUploadWithHeaders() throws Exception {

        mockUpload();

        //#upload
        final Sink<ByteString, CompletionStage<MultipartUploadResult>> sink = client.multipartUpload(bucket(), bucketKey(), akka.http.javadsl.model.headers.ContentType.);
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
}
