/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.jakartajms
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

  override def equals(other: Any): Boolean = other match {
    case that: Credentials =>
      java.util.Objects.equals(this.username, that.username) &&
      java.util.Objects.equals(this.password, that.password)
    case _ => false
  }

  override def hashCode(): Int = java.util.Objects.hash(username, password)
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
