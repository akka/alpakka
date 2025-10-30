/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.storage

import java.util.Optional
import scala.jdk.OptionConverters._

final class Owner private (entity: String, entityId: Option[String]) {
  def withEntity(entity: String): Owner = copy(entity = entity)
  def withEntityId(entityId: String): Owner = copy(entityId = Option(entityId))

  /** Java API */
  def getEntityId: Optional[String] = entityId.toJava

  private def copy(entity: String = entity, entityId: Option[String] = entityId): Owner =
    new Owner(entity, entityId)

  override def toString: String =
    s"Owner(entity=$entity, entityId=$entityId)"
}

object Owner {

  /** Scala API */
  def apply(entity: String, entityId: Option[String]): Owner =
    new Owner(entity, entityId)

  /** Java API */
  def create(entity: String, entityId: Optional[String]): Owner =
    new Owner(entity, entityId.toScala)
}
