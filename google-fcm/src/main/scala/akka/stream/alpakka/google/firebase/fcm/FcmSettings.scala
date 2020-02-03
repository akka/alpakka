/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

final class FcmSettings private (
    val clientEmail: String,
    val privateKey: String,
    val projectId: String,
    val isTest: Boolean,
    val maxConcurrentConnections: Int
) {

  def withClientEmail(value: String): FcmSettings = copy(clientEmail = value)
  def withPrivateKey(value: String): FcmSettings = copy(privateKey = value)
  def withProjectId(value: String): FcmSettings = copy(projectId = value)
  def withIsTest(value: Boolean): FcmSettings = if (isTest == value) this else copy(isTest = value)
  def withMaxConcurrentConnections(value: Int): FcmSettings = copy(maxConcurrentConnections = value)

  private def copy(
      clientEmail: String = clientEmail,
      privateKey: String = privateKey,
      projectId: String = projectId,
      isTest: Boolean = isTest,
      maxConcurrentConnections: Int = maxConcurrentConnections
  ): FcmSettings =
    new FcmSettings(clientEmail = clientEmail,
                    privateKey = privateKey,
                    projectId = projectId,
                    isTest = isTest,
                    maxConcurrentConnections = maxConcurrentConnections)

  override def toString =
    s"""FcmFlowConfig(clientEmail=$clientEmail,projectId=$projectId,isTest=$isTest,maxConcurrentConnections=$maxConcurrentConnections)"""

}

object FcmSettings {

  /** Scala API */
  def apply(
      clientEmail: String,
      privateKey: String,
      projectId: String
  ): FcmSettings = new FcmSettings(
    clientEmail,
    privateKey,
    projectId,
    isTest = false,
    maxConcurrentConnections = 100
  )

  /** Java API */
  def create(
      clientEmail: String,
      privateKey: String,
      projectId: String
  ): FcmSettings = new FcmSettings(
    clientEmail,
    privateKey,
    projectId,
    isTest = false,
    maxConcurrentConnections = 100
  )
}
