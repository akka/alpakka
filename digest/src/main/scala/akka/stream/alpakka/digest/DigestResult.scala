/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.digest

import akka.Done
import akka.util.ByteString

import scala.util.Try

final case class DigestResult(messageDigest: ByteString, messageDigestAsHexString: String, status: Try[Done])
