/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.headers.ByteRange;
import akka.http.scaladsl.model.headers.RawHeader;
import akka.stream.alpakka.azure.storage.headers.ServerSideEncryption;
import akka.stream.alpakka.azure.storage.requests.CreateFile;
import akka.stream.alpakka.azure.storage.requests.GetBlob;
import akka.stream.alpakka.azure.storage.requests.PutBlockBlob;
import akka.stream.alpakka.testkit.javadsl.LogCapturingJunit4;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import scala.Option;

public class RequestBuilderTest {

    @Rule
    public final LogCapturingJunit4 logCapturing = new LogCapturingJunit4();

    @Test
    public void createSimpleRequest() {

        //#simple-request-builder
        final GetBlob requestBuilder = GetBlob.create();
        //#simple-request-builder

        Assert.assertEquals(Option.empty(), requestBuilder.versionId());
        Assert.assertEquals(Option.empty(), requestBuilder.range());
        Assert.assertEquals(Option.empty(), requestBuilder.leaseId());
        Assert.assertEquals(Option.empty(), requestBuilder.sse());
    }

    @Test
    public void populateRequestBuilder() {

        //#populate-request-builder
        final var requestBuilder = GetBlob.create().withLeaseId("my-lease-id").withRange(ByteRange.createSlice(0, 25));
        //#populate-request-builder

        Assert.assertEquals(Option.apply("my-lease-id"), requestBuilder.leaseId());
        Assert.assertEquals(Option.apply(ByteRange.createSlice(0, 25)), requestBuilder.range());
        Assert.assertEquals(Option.empty(), requestBuilder.sse());
    }

    @Test
    public void createRequestBuilderWithMandatoryParams() {

        //#request-builder-with-initial-values
        final var requestBuilder = CreateFile.create(256L, ContentTypes.TEXT_PLAIN_UTF8);
        //#request-builder-with-initial-values

        Assert.assertEquals(Option.empty(), requestBuilder.leaseId());
        Assert.assertEquals(256L, requestBuilder.maxFileSize());
        Assert.assertEquals(ContentTypes.TEXT_PLAIN_UTF8, requestBuilder.contentType());
    }

    @Test
    public void populateServerSideEncryption() {

        //#request-builder-with-sse
        final var requestBuilder = PutBlockBlob.create(256L, ContentTypes.TEXT_PLAIN_UTF8)
                .withServerSideEncryption(ServerSideEncryption.customerKey("SGVsbG9Xb3JsZA=="));
        //#request-builder-with-sse

        Assert.assertEquals(ServerSideEncryption.customerKey("SGVsbG9Xb3JsZA=="), requestBuilder.sse().get());
    }

    @Test
    public void populateAdditionalHeaders() {

        //#request-builder-with-additional-headers
        final var requestBuilder = GetBlob.create().addHeader("If-Match", "foobar");
        //#request-builder-with-additional-headers

        Assert.assertEquals(1, requestBuilder.additionalHeaders().size());
        Assert.assertEquals(new RawHeader("If-Match", "foobar"), requestBuilder.additionalHeaders().head());
    }
}
