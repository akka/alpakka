/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.dynamodb

import com.amazonaws._
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.transform.Marshaller

/**
 * Provide conversion and marshalling for [[com.amazonaws.AmazonWebServiceRequest]].
 */
trait AwsOp {

  /** Type of the request. */
  type A <: AmazonWebServiceRequest

  /** Type of the reply. */
  type B <: AmazonWebServiceResult[ResponseMetadata]

  /** The request instance to be sent. */
  val request: A

  val handler: HttpResponseHandler[AmazonWebServiceResponse[B]]

  val marshaller: Marshaller[Request[A], A]
}

trait AwsPagedOp extends AwsOp {
  def next(a: A, b: B): Option[AwsPagedOp]
}
