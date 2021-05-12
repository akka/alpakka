/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.scaladsl

/**
 * Models a paginated resource
 * @tparam T the resource
 */
trait Paginated[-T] {

  /**
   * Returns the token for the next page, if present
   * @param resource the paginated resource
   */
  def pageToken(resource: T): Option[String]
}
