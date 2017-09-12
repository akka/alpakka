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

import scala.Option;
import scala.Some;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DocSnippets extends S3WireMockBase {

    final Materializer materializer = ActorMaterializer.create(system());

    // Documentation snippet only
    public void connectBluemix() {
        final Materializer mat = ActorMaterializer.create(system());
        // #java-bluemix-example
        final String host = "s3.eu-geo.objectstorage.softlayer.net";
        final int port = 443;

        final AWSCredentials credentials = new BasicCredentials( "myAccessKeyId","mySecretAccessKey");
        final Proxy proxy = new Proxy(host, port, "https");

        // Set pathStyleAccess to true and specify proxy, leave region blank
        final S3Settings settings = new S3Settings(MemoryBufferType.getInstance(), "", Some.apply(proxy), credentials, "", true);
        final S3Client s3Client = new S3Client(settings,system(), mat);
        // #java-bluemix-example
    }
}
