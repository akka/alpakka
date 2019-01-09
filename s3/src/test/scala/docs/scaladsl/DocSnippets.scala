/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package docs.scaladsl

import akka.stream.alpakka.s3._
import akka.stream.alpakka.s3.scaladsl.S3
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.AwsRegionProvider

object DoucmentationSnippets {

  def connectBluemix(): Unit = {
    // #scala-bluemix-example
    val host = "s3.eu-geo.objectstorage.softlayer.net"
    val port = 443

    val credentialsProvider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(
        "myAccessKeyId",
        "mySecretAccessKey"
      )
    )
    val regionProvider = new AwsRegionProvider {
      def getRegion = ""
    }
    val proxy = Some(Proxy(host, port, "https"))

    // Set pathStyleAccess to true and specify proxy, leave region blank
    val bluemix =
      S3Settings(MemoryBufferType, proxy, credentialsProvider, regionProvider, true, None, ListBucketVersion2)
    // FIXME
    S3.listBucket("my-bucket", None).withAttributes(S3Attributes.settings(bluemix))
    // #scala-bluemix-example
  }
}
