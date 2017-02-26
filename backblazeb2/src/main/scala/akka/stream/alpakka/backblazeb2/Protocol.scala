/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.backblazeb2

object Protocol {
  case class AccountAuthorizationToken(value: String) extends AnyVal
  case class UploadAuthorizationToken(value: String) extends AnyVal

  case class AuthorizeAccountResponse(authorizationToken: AccountAuthorizationToken, apiUrl: String)
}
