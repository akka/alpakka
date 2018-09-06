/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm

final class FcmFlowConfig private (
    val clientEmail: String,
    val privateKey: String,
    val projectid: String,
    val isTest: Boolean,
    val maxConcurentConnections: Int
) {

  def withClientEmail(value: String): FcmFlowConfig = copy(clientEmail = value)
  def withPrivateKey(value: String): FcmFlowConfig = copy(privateKey = value)
  def withProjectid(value: String): FcmFlowConfig = copy(projectid = value)
  def withIsTest(value: Boolean): FcmFlowConfig = if (isTest == value) this else copy(isTest = value)
  def withMaxConcurentConnections(value: Int): FcmFlowConfig = copy(maxConcurentConnections = value)

  private def copy(
      clientEmail: String = clientEmail,
      privateKey: String = privateKey,
      projectid: String = projectid,
      isTest: Boolean = isTest,
      maxConcurentConnections: Int = maxConcurentConnections
  ): FcmFlowConfig =
    new FcmFlowConfig(clientEmail = clientEmail,
                      privateKey = privateKey,
                      projectid = projectid,
                      isTest = isTest,
                      maxConcurentConnections = maxConcurentConnections)

  override def toString =
    s"""FcmFlowConfig(clientEmail=$clientEmail,projectid=$projectid,isTest=$isTest,maxConcurentConnections=$maxConcurentConnections)"""

}

object FcmFlowConfig {

  /** Scala API */
  def apply(
      clientEmail: String,
      privateKey: String,
      projectid: String,
  ): FcmFlowConfig = new FcmFlowConfig(
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
  ): FcmFlowConfig = new FcmFlowConfig(
    clientEmail,
    privateKey,
    projectid,
    isTest = false,
    maxConcurentConnections = 100
  )
}
