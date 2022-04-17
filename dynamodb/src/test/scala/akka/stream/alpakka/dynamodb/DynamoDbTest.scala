/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import akka.http.scaladsl.Http
import akka.testkit.TestKitBase
import com.dimafeng.testcontainers.ForAllTestContainer
import org.scalatest.{BeforeAndAfterAll, Suite}

//#init-client
import akka.actor.ActorSystem

import com.github.matsluni.akkahttpspi.AkkaHttpClient
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient

//#init-client

trait DynamoDbTest extends ForAllTestContainer with TestKitBase with BeforeAndAfterAll { self: Suite =>
  implicit val system: ActorSystem
  override lazy val container: DynamoDbLocalContainer = new DynamoDbLocalContainer()

  def dynamoDbAsyncClient: DynamoDbAsyncClient = {
    val credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x"))
    //#init-client

    // Don't encode credentials in your source code!
    // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
    implicit val client: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .region(Region.AWS_GLOBAL)
      .credentialsProvider(credentialsProvider)
      .httpClient(AkkaHttpClient.builder().withActorSystem(system).build())
      // Possibility to configure the retry policy
      // see https://doc.akka.io/docs/alpakka/current/aws-shared-configuration.html
      // .overrideConfiguration(...)
      //#init-client
      .endpointOverride(container.uri)
      //#init-client
      .build()

    system.registerOnTermination(client.close())
    //#init-client
    client
  }

  override protected def afterAll(): Unit = {
    Http(system)
      .shutdownAllConnectionPools()
      .foreach { _ =>
        shutdown()
      }(system.dispatcher)
    super.afterAll()
  }

}
