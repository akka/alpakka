/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.storage.GoogleAuthConfiguration
import akka.stream.alpakka.googlecloud.storage.Model.{BucketInfo, StorageObject}
import akka.stream.alpakka.googlecloud.storage.impl.GoogleCloudStorageClient
import akka.stream.javadsl.Source
import com.typesafe.config.ConfigFactory

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._

object GoogleCloudStorage {
  def create(system: ActorSystem, mat: Materializer): GoogleCloudStorage = {
    val configuration: GoogleAuthConfiguration = GoogleAuthConfiguration(ConfigFactory.load()).getOrElse(
      throw new RuntimeException("Failed to load configuration")
    )
    new GoogleCloudStorage(configuration, system, mat)
  }
}

final class GoogleCloudStorage(configuration: GoogleAuthConfiguration, system: ActorSystem, mat: Materializer) {

  private val impl = new GoogleCloudStorageClient(configuration)(system, mat)

  /**
   * Gets information on a bucket
   *
   * @param bucketName the name of the bucket to look up
   * @return the [[BucketInfo]] if it exists
   */
  def getBucket(bucketName: String): CompletionStage[Optional[BucketInfo]] =
    impl.getBucket(bucketName).map(_.asJava)(mat.executionContext).toJava

  /**
   * Creates a new bucket
   *
   * @param bucketName the name of the bucket
   * @param location the region to put the bucket in
   * @return the [[BucketInfo]] for the created bucket
   */
  def createBucket(bucketName: String, location: String): CompletionStage[BucketInfo] =
    impl.createBucket(bucketName, location).toJava

  /**
   * Deletes a bucket
   *
   * @param bucketName the name of the bucket
   * @return the [[BucketInfo]] for the created bucket
   */
  def createBucket(bucketName: String): CompletionStage[Done] =
    impl.deleteBucket(bucketName).toJava

  /**
   * Lists the bucket contents
   * @param bucket the bucket name
   * @return a [[Stream]] of [[StorageObject]]
   */
  def listBucket(bucket: String): Source[StorageObject, NotUsed] =
    impl.listBucket(bucket, None).asJava

  /**
   * Lists the bucket contents
   * @param bucket the bucket name
   * @param prefix the bucket prefix
   * @return a [[Stream]] of [[StorageObject]]
   */
  def listBucket(bucket: String, prefix: String): Source[StorageObject, NotUsed] =
    impl.listBucket(bucket, Option(prefix)).asJava
}
