/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.storage

@deprecated("Use akka.stream.alpakka.google.GoogleSettings", "3.0.0")
final class StorageSettings private (val projectId: String, val clientEmail: String, val privateKey: String) {
  def withProjectId(projectId: String): StorageSettings = copy(projectId = projectId)

  def withClientEmail(clientEmail: String): StorageSettings = copy(clientEmail = clientEmail)

  def withPrivateKey(privateKey: String): StorageSettings = copy(privateKey = privateKey)

  private def copy(projectId: String = projectId,
                   clientEmail: String = clientEmail,
                   privateKey: String = privateKey): StorageSettings =
    new StorageSettings(projectId, clientEmail, privateKey)

  override def toString: String =
    s"StorageSettings(projectId=$projectId, clientEmail=$clientEmail, privateKey=**)"
}

@deprecated("Use akka.stream.alpakka.google.GoogleSettings", "3.0.0")
object StorageSettings {
  def apply(projectId: String, clientEmail: String, privateKey: String): StorageSettings =
    new StorageSettings(projectId, clientEmail, privateKey)

  def create(projectId: String, clientEmail: String, privateKey: String): StorageSettings =
    new StorageSettings(projectId, clientEmail, privateKey)
}
