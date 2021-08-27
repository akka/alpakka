/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.firebase.fcm.v1.models

sealed trait FcmResponse {
  def isFailure: Boolean
  def isSuccess: Boolean
}

final case class FcmSuccessResponse(name: String) extends FcmResponse {
  val isFailure = false
  val isSuccess = true
  def getName: String = name
}

final case class FcmErrorResponse(rawError: String) extends FcmResponse {
  val isFailure = true
  val isSuccess = false
  def getRawError: String = rawError
}
