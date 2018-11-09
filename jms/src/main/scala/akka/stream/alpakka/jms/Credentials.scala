/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.jms
import com.typesafe.config.Config

final class Credentials private (
    val username: String,
    val password: String
) {

  def withUsername(value: String): Credentials = copy(username = value)
  def withPassword(value: String): Credentials = copy(password = value)

  private def copy(
      username: String = username,
      password: String = password
  ): Credentials = new Credentials(
    username = username,
    password = password
  )

  override def toString =
    "Credentials(" +
    s"username=$username," +
    s"password=${"*" * password.length}" +
    ")"
}

object Credentials {

  /**
   * Reads from the given config.
   */
  def apply(c: Config): Credentials = {
    val username = c.getString("username")
    val password = c.getString("password")
    new Credentials(
      username,
      password
    )
  }

  /**
   * Java API: Reads from the given config.
   */
  def create(c: Config): Credentials = apply(c)

  /** Scala API */
  def apply(
      username: String,
      password: String
  ): Credentials = new Credentials(
    username,
    password
  )

  /** Java API */
  def create(
      username: String,
      password: String
  ): Credentials = new Credentials(
    username,
    password
  )
}
