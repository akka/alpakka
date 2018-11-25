/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

/**
 * Represents a bucket in Google Cloud Storage.
 *
 * @param name The name of the bucket
 * @param location The location of the bucket, object data for objects in the bucket resides in physical storage within this region, Defaults to US.
 * @param kind The kind of item this is
 * @param id The ID of the bucket
 * @param selfLink The URI of this bucket
 * @param etag HTTP 1.1 Entity tag for the bucket
 */
final class Bucket private (
    val name: String,
    val location: String,
    val kind: String,
    val id: String,
    val selfLink: String,
    val etag: String
) {

  /** Java API */
  def getName: String = name

  /** Java API */
  def getLocation: String = location

  /** Java API */
  def getKind: String = kind

  /** Java API */
  def getId: String = id

  /** Java API */
  def getSelfLink: String = selfLink

  /** Java API */
  def getEtag: String = etag

  def withName(value: String): Bucket = copy(name = value)
  def withLocation(value: String): Bucket = copy(location = value)
  def withKind(value: String): Bucket = copy(kind = value)
  def withId(value: String): Bucket = copy(id = value)
  def withSelfLink(value: String): Bucket = copy(selfLink = value)
  def withEtag(value: String): Bucket = copy(etag = value)

  private def copy(
      name: String = name,
      location: String = location,
      kind: String = kind,
      id: String = id,
      selfLink: String = selfLink,
      etag: String = etag
  ): Bucket = new Bucket(
    name = name,
    location = location,
    kind = kind,
    id = id,
    selfLink = selfLink,
    etag = etag
  )

  override def toString =
    "BucketInfo(" +
    s"name=$name," +
    s"location=$location," +
    s"kind=$kind," +
    s"id=$id," +
    s"selfLink=$selfLink," +
    s"etag=$etag" +
    ")"

  override def equals(other: Any): Boolean = other match {
    case that: Bucket =>
      java.util.Objects.equals(this.name, that.name) &&
      java.util.Objects.equals(this.location, that.location) &&
      java.util.Objects.equals(this.kind, that.kind) &&
      java.util.Objects.equals(this.id, that.id) &&
      java.util.Objects.equals(this.selfLink, that.selfLink) &&
      java.util.Objects.equals(this.etag, that.etag)
    case _ => false
  }

  override def hashCode(): Int =
    java.util.Objects.hash(name, location, kind, id, selfLink, etag)
}
object Bucket {

  /** Scala API */
  def apply(
      name: String,
      location: String,
      kind: String,
      id: String,
      selfLink: String,
      etag: String
  ): Bucket = new Bucket(
    name,
    location,
    kind,
    id,
    selfLink,
    etag
  )

  /** Java API */
  def create(
      name: String,
      location: String,
      kind: String,
      id: String,
      selfLink: String,
      etag: String
  ): Bucket = new Bucket(
    name,
    location,
    kind,
    id,
    selfLink,
    etag
  )
}
