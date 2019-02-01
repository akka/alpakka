/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

final class FcmSettings private (
    val clientEmail: String,
    val privateKey: String,
    val projectid: String,
    val isTest: Boolean,
    val maxConcurentConnections: Int
) {

  def withClientEmail(value: String): FcmSettings = copy(clientEmail = value)
  def withPrivateKey(value: String): FcmSettings = copy(privateKey = value)
  def withProjectid(value: String): FcmSettings = copy(projectid = value)
  def withIsTest(value: Boolean): FcmSettings = if (isTest == value) this else copy(isTest = value)
  def withMaxConcurentConnections(value: Int): FcmSettings = copy(maxConcurentConnections = value)

  private def copy(
      clientEmail: String = clientEmail,
      privateKey: String = privateKey,
      projectid: String = projectid,
      isTest: Boolean = isTest,
      maxConcurentConnections: Int = maxConcurentConnections
  ): FcmSettings =
    new FcmSettings(clientEmail = clientEmail,
                    privateKey = privateKey,
                    projectid = projectid,
                    isTest = isTest,
                    maxConcurentConnections = maxConcurentConnections)

  override def toString =
    s"""FcmFlowConfig(clientEmail=$clientEmail,projectid=$projectid,isTest=$isTest,maxConcurentConnections=$maxConcurentConnections)"""

}

object FcmSettings {

  /** Scala API */
  def apply(
      clientEmail: String,
      privateKey: String,
      projectid: String,
  ): FcmSettings = new FcmSettings(
    clientEmail,
    privateKey,
    projectid,
    isTest = false,
    maxConcurentConnections = 100
  )

  /** Java API */
  def create(
      clientEmail: String,
      privateKey: String,
      projectid: String,
  ): FcmSettings = new FcmSettings(
    clientEmail,
    privateKey,
    projectid,
    isTest = false,
    maxConcurentConnections = 100
  )
}
