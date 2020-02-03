/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package main.scala.akka.stream.alpakka.googlecloud.storage

final class ObjectAccessControls private (
    kind: String,
    id: String,
    selfLink: String,
    bucket: String,
    `object`: String,
    generation: String,
    entity: String,
    role: String,
    email: String,
    entityId: String,
    domain: String,
    projectTeam: ProjectTeam,
    etag: String
) {
  def withKind(kind: String): ObjectAccessControls = copy(kind = kind)
  def withId(id: String): ObjectAccessControls = copy(id = id)
  def withSelfLink(selfLink: String): ObjectAccessControls = copy(selfLink = selfLink)
  def withBucket(bucket: String): ObjectAccessControls = copy(bucket = bucket)
  def withObject(`object`: String): ObjectAccessControls = copy(`object` = `object`)
  def withGeneration(generation: String): ObjectAccessControls = copy(generation = generation)
  def withEntity(entity: String): ObjectAccessControls = copy(entity = entity)
  def withRole(role: String): ObjectAccessControls = copy(role = role)
  def withEmail(email: String): ObjectAccessControls = copy(email = email)
  def withEntityId(entityId: String): ObjectAccessControls = copy(entityId = entityId)
  def withDomain(domain: String): ObjectAccessControls = copy(domain = domain)
  def withProjectTeam(projectTeam: ProjectTeam): ObjectAccessControls = copy(projectTeam = projectTeam)
  def withEtag(etag: String): ObjectAccessControls = copy(etag = etag)

  private def copy(kind: String = kind,
                   id: String = id,
                   selfLink: String = selfLink,
                   bucket: String = bucket,
                   `object`: String = `object`,
                   generation: String = generation,
                   entity: String = entity,
                   role: String = role,
                   email: String = email,
                   entityId: String = entityId,
                   domain: String = domain,
                   projectTeam: ProjectTeam = projectTeam,
                   etag: String = etag): ObjectAccessControls =
    new ObjectAccessControls(kind,
                             id,
                             selfLink,
                             bucket,
                             `object`,
                             generation,
                             entity,
                             role,
                             email,
                             entityId,
                             domain,
                             projectTeam,
                             etag)

  override def toString: String =
    s"CustomerEncryption(" +
    s"kind=$kind," +
    s"id=$id," +
    s"selfLink=$selfLink," +
    s"bucket=$bucket," +
    s"object=${`object`}," +
    s"generation=$generation," +
    s"entity=$entity," +
    s"role=$role," +
    s"email=$email," +
    s"entityId=$entityId," +
    s"domain=$domain," +
    s"projectTeam=$projectTeam," +
    s"etag=$etag)"
}

object ObjectAccessControls {
  def apply(kind: String,
            id: String,
            selfLink: String,
            bucket: String,
            `object`: String,
            generation: String,
            entity: String,
            role: String,
            email: String,
            entityId: String,
            domain: String,
            projectTeam: ProjectTeam,
            etag: String): ObjectAccessControls =
    new ObjectAccessControls(kind,
                             id,
                             selfLink,
                             bucket,
                             `object`,
                             generation,
                             entity,
                             role,
                             email,
                             entityId,
                             domain,
                             projectTeam,
                             etag)

  def create(kind: String,
             id: String,
             selfLink: String,
             bucket: String,
             `object`: String,
             generation: String,
             entity: String,
             role: String,
             email: String,
             entityId: String,
             domain: String,
             projectTeam: ProjectTeam,
             etag: String): ObjectAccessControls =
    new ObjectAccessControls(kind,
                             id,
                             selfLink,
                             bucket,
                             `object`,
                             generation,
                             entity,
                             role,
                             email,
                             entityId,
                             domain,
                             projectTeam,
                             etag)
}
