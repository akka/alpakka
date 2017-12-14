/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage.scaladsl

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.alpakka.googlecloud.storage.GoogleAuthConfiguration
import akka.stream.alpakka.googlecloud.storage.Model.{BucketInfo, StorageObject}
import akka.stream.alpakka.googlecloud.storage.impl.GoogleCloudStorageClient
import akka.stream.scaladsl.Source

import scala.concurrent.Future

final class GoogleCloudStorage(configuration: GoogleAuthConfiguration)(implicit system: ActorSystem, mat: Materializer) {

  private val impl = new GoogleCloudStorageClient(configuration)(system, mat)

  /**
   * Gets information on a bucket
   *
   * @param bucketName the name of the bucket to look up
   * @return the [[BucketInfo]] if it exists
   */
  def getBucket(bucketName: String): Future[Option[BucketInfo]] =
    impl.getBucket(bucketName)

  /**
   * Creates a new bucket
   *
   * @param bucketName the name of the bucket
   * @param location the region to put the bucket in
   * @return the [[BucketInfo]] for the created bucket
   */
  def createBucket(bucketName: String, location: String): Future[BucketInfo] =
    impl.createBucket(bucketName, location)

  /**
   * Creates a new bucket
   *
   * @param bucketName the name of the bucket
   * @return the [[BucketInfo]] for the created bucket
   */
  def deleteBucket(bucketName: String): Future[Done] =
    impl.deleteBucket(bucketName)

  /**
   * Lists the bucket contents
   * @param bucket the bucket name
   * @param prefix the bucket prefix
   * @return a [[Stream]] of [[StorageObject]]
   */
  def listBucket(bucket: String, prefix: Option[String] = None): Source[StorageObject, NotUsed] =
    impl.listBucket(bucket, prefix)

}
