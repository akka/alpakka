/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.util.concurrent.CompletionStage;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.s3.MemoryBufferType;
import akka.stream.alpakka.s3.S3Client;
import akka.stream.alpakka.s3.S3Settings;
import akka.stream.alpakka.s3.acl.CannedAcl;
import akka.stream.alpakka.s3.impl.ListBucketVersion2;
import akka.stream.alpakka.s3.impl.S3Headers;
import akka.stream.alpakka.s3.impl.ServerSideEncryption;
import akka.stream.alpakka.s3.javadsl.MultipartUploadResult;
import akka.stream.alpakka.s3.javadsl.S3External;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.AwsRegionProvider;

public class JavaExamplesSnippets {

  // Java examples documentation snippet only

  private static ActorSystem system = ActorSystem.create();
  private static Materializer materializer = ActorMaterializer.create(system);

  private final AWSCredentialsProvider credentials =
      new AWSStaticCredentialsProvider(
          new BasicAWSCredentials("my-AWS-access-key-ID", "my-AWS-password"));

  private AwsRegionProvider regionProvider(String region) {
    return new AwsRegionProvider() {
      @Override
      public String getRegion() throws SdkClientException {
        return region;
      }
    };
  }

  private final S3Settings settings =
      new S3Settings(
          MemoryBufferType.getInstance(),
          null,
          credentials,
          regionProvider("us-east-1"),
          false,
          scala.Option.empty(),
          ListBucketVersion2.getInstance());

  private final S3Client client = S3Client.create(settings, system, materializer);

  public void aes256Encryption(
      String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
    // #java-example
    // setting the encryption to AES256
    CompletionStage<MultipartUploadResult> result =
        S3External.multipartCopy(
            sourceBucket,
            sourceKey,
            targetBucket,
            targetKey,
            S3Headers.empty(),
            ServerSideEncryption.AES256$.MODULE$,
            client);
    // #java-example
  }

  public void cannedAcl(
      String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
    // #java-example

    // using canned ACL
    CompletionStage<MultipartUploadResult> result =
        S3External.multipartCopy(
            sourceBucket,
            sourceKey,
            targetBucket,
            targetKey,
            S3Headers.apply(CannedAcl.Private$.MODULE$),
            null, // encryption,
            client);
    // #java-example
  }
}
