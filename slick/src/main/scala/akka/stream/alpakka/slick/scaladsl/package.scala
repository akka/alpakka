/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.slick

package object scaladsl {

  /**
   * Scala API: Represents an "open" Slick database and its database (type) profile.
   *
   * <b>NOTE</b>: these databases need to be closed after creation to
   * avoid leaking database resources like active connection pools, etc.
   */
  type SlickSession = javadsl.SlickSession

  /**
   * Scala API: Methods for "opening" Slick databases for use.
   *
   * <b>NOTE</b>: databases created through these methods will need to be
   * closed after creation to avoid leaking database resources like active
   * connection pools, etc.
   */
  val SlickSession = javadsl.SlickSession
}
