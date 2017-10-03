/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.oracle.bmcs.javadsl;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.alpakka.oracle.bmcs.BmcsSettings;
import akka.stream.alpakka.oracle.bmcs.BufferType;
import akka.stream.alpakka.oracle.bmcs.MemoryBufferType;
import akka.stream.alpakka.oracle.bmcs.auth.BasicCredentials;
import akka.stream.alpakka.oracle.bmcs.auth.BmcsCredentials;
import akka.stream.alpakka.oracle.bmcs.javadsl.BmcsClient;
import akka.stream.alpakka.oracle.bmcs.javadsl.MultipartUploadResult;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

public class BmcsJavaClientTest {

    private ActorSystem system = ActorSystem.create();

    private final Materializer materializer =
            ActorMaterializer.create(system);


    private final String userOcid = "ocid1.user.oc1..aaaaaaaaalwxriuznfhohggk7ejii6lpwo7mebuldxh455hiesnowaoaksyq";
    private final String keyFingerprint = "cb:17:e0:45:d0:24:d3:ff:be:ee:1b:0e:f8:2c:58:27";
    private final String tenancyOcid = "ocid1.tenancy.oc1..aaaaaaaa6gtmn46bketftho3sqcgrlvdfsenqemqy3urkbthlpkos54a6wsa";
    private final String keyPath = "SOME_KEY_PATH";
    private final String passphrase = "aditya";
    private final BmcsCredentials cred = BasicCredentials.create(userOcid, tenancyOcid, keyPath, passphrase, keyFingerprint);
    //create settings. overriding default region and namespace.
    private final BmcsSettings settings = BmcsSettings.create("us-phoenix-1", "oraclegbudev", MemoryBufferType.getInstance());

    private final BmcsClient client = BmcsClient.create(settings, cred, system , materializer);

    private final String bucket = "CEGBU_Prime";
    private final int chunkSize = 5 * 1024 * 1024;


    @Ignore
    @Test
    public void testMultipartUpload() throws ExecutionException, InterruptedException {

        String objectName = "BmcsNoMockTest-" + UUID.randomUUID().toString().substring(0, 5);
        try (InputStream in = new FileInputStream(new File("/ssd/scala_github/alpakka/ehs.pdf"))) {
            Source<ByteString, CompletionStage<IOResult>> src = StreamConverters.fromInputStream(() -> in);
            Sink<ByteString, CompletionStage<MultipartUploadResult>> sink = client.multipartUpload(bucket, objectName, chunkSize, 4);
            CompletionStage<MultipartUploadResult> uploadStage = src.runWith(sink, materializer);
            MultipartUploadResult multipartUploadResult = uploadStage.toCompletableFuture().get();
            Assert.assertNotNull(multipartUploadResult.etag());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
