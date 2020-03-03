/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

final class Owner private (entity: String, entityId: String) {
  def withEntity(entity: String): Owner = copy(entity = entity)
  def withEntityId(entityId: String): Owner = copy(entityId = entityId)

  private def copy(entity: String = entity, entityId: String = entityId): Owner =
    new Owner(entity, entityId)

  override def toString: String =
    s"Owner(entity=$entity, entityId=$entityId)"
}

object Owner {
  def apply(entity: String, entityId: String): Owner =
    new Owner(entity, entityId)

  def create(entity: String, entityId: String): Owner =
    new Owner(entity, entityId)
}
