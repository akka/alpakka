/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.javadsl;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.alpakka.s3.MemoryBufferType;
import akka.stream.alpakka.s3.S3Settings;
import akka.stream.alpakka.s3.acl.CannedAcl;
import akka.stream.alpakka.s3.ListBucketVersion2;
import akka.stream.alpakka.s3.impl.S3Headers;
import akka.stream.alpakka.s3.impl.ServerSideEncryption;
import akka.stream.alpakka.s3.javadsl.MultipartUploadResult;
import akka.stream.alpakka.s3.javadsl.S3;
import akka.stream.javadsl.RunnableGraph;
import akka.stream.javadsl.Source;
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
      S3Settings.create(
          MemoryBufferType.getInstance(),
          Optional.empty(),
          credentials,
          regionProvider("us-east-1"),
          false,
          Optional.empty(),
          ListBucketVersion2.getInstance());

  public void aes256Encryption(
      String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
    // #java-example
    // setting the encryption to AES256
    RunnableGraph<Source<MultipartUploadResult, NotUsed>> result =
        S3.multipartCopy(
            sourceBucket,
            sourceKey,
            targetBucket,
            targetKey,
            S3Headers.empty(),
            ServerSideEncryption.AES256$.MODULE$);
    // #java-example
  }

  public void cannedAcl(
      String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
    // #java-example

    // using canned ACL
    RunnableGraph<Source<MultipartUploadResult, NotUsed>> result =
        S3.multipartCopy(
            sourceBucket,
            sourceKey,
            targetBucket,
            targetKey,
            S3Headers.apply(CannedAcl.Private$.MODULE$),
            null); // encryption
    // #java-example
  }
}
