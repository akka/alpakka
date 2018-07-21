/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.impl.ListBucketVersion2
import akka.stream.alpakka.s3.{MemoryBufferType, Proxy, S3Settings}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.AwsRegionProvider

object DoucmentationSnippets {

  def connectBluemix(): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

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
    val settings =
      new S3Settings(MemoryBufferType, proxy, credentialsProvider, regionProvider, true, None, ListBucketVersion2)
    val s3Client = new S3Client(settings)(system, materializer)
    // #scala-bluemix-example
  }
}
