/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka

import com.amazonaws._
import com.amazonaws.http.HttpResponseHandler
import com.amazonaws.transform.Marshaller

package dynamodb {

  trait AwsOp {
    type A <: AmazonWebServiceRequest
    type B <: AmazonWebServiceResult[ResponseMetadata]
    val request: A
    val handler: HttpResponseHandler[AmazonWebServiceResponse[B]]
    val marshaller: Marshaller[Request[A], A]
  }

}
