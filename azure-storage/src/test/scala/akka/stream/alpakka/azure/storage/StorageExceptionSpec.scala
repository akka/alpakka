/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka
package azure
package storage

import akka.http.scaladsl.model.StatusCodes
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

class StorageExceptionSpec extends AnyFlatSpecLike with Matchers {

  "Storage exception" should "be parsed" in {
    val ex = StorageException("invalid xml", StatusCodes.NotFound)
    ex.toString shouldBe """StorageException(statusCode=404 Not Found, errorCode=Content is not allowed in prolog.,
                           | errorMessage=invalid xml, resourceName=None, resourceValue=None, reason=None)
                           |""".stripMargin.replaceAll(System.lineSeparator(), "")
  }

  it should "parse XML containing error related to query" in {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<Error>
        |  <Code>InvalidQueryParameterValue</Code>
        |  <Message>Value for one of the query parameters specified in the request URI is invalid.</Message>
        |  <QueryParameterName>popreceipt</QueryParameterName>
        |  <QueryParameterValue>33537277-6a52-4a2b-b4eb-0f905051827b</QueryParameterValue>
        |  <Reason>invalid receipt format</Reason>
        |</Error>
        |""".stripMargin

    StorageException(xml, StatusCodes.BadRequest) shouldBe StorageException(
      statusCode = StatusCodes.BadRequest,
      errorCode = "InvalidQueryParameterValue",
      errorMessage = "Value for one of the query parameters specified in the request URI is invalid.",
      resourceName = Some("popreceipt"),
      resourceValue = Some("33537277-6a52-4a2b-b4eb-0f905051827b"),
      reason = Some("invalid receipt format")
    )
  }

  it should "parse XML containing error related to header" in {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<Error>
        |  <Code>InvalidHeaderValue</Code>
        |  <Message>Invalid header</Message>
        |  <HeaderName>popreceipt</HeaderName>
        |  <HeaderValue>33537277-6a52-4a2b-b4eb-0f905051827b</HeaderValue>
        |  <Reason>invalid receipt format</Reason>
        |</Error>
        |""".stripMargin

    StorageException(xml, StatusCodes.BadRequest) shouldBe StorageException(
      statusCode = StatusCodes.BadRequest,
      errorCode = "InvalidHeaderValue",
      errorMessage = "Invalid header",
      resourceName = Some("popreceipt"),
      resourceValue = Some("33537277-6a52-4a2b-b4eb-0f905051827b"),
      reason = Some("invalid receipt format")
    )
  }

  it should "parse XML containing error related to authentication" in {
    val xml =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<Error>
        |  <Code>InvalidAuthenticationInfo</Code>
        |  <Message>Authentication failed</Message>
        |  <AuthenticationErrorDetail>Server failed to authenticate the request. Please refer to the information in the www-authenticate header.</AuthenticationErrorDetail>
        |</Error>
        |""".stripMargin

    StorageException(xml, StatusCodes.Unauthorized) shouldBe StorageException(
      statusCode = StatusCodes.Unauthorized,
      errorCode = "InvalidAuthenticationInfo",
      errorMessage = "Authentication failed",
      resourceName = None,
      resourceValue = None,
      reason = Some(
        "Server failed to authenticate the request. Please refer to the information in the www-authenticate header."
      )
    )
  }

  it should "survive null" in {
    StorageException(null, StatusCodes.ServiceUnavailable) shouldBe
    StorageException(statusCode = StatusCodes.ServiceUnavailable,
                     errorCode = "null",
                     errorMessage = "null",
                     resourceName = None,
                     resourceValue = None,
                     reason = None)
  }
}
