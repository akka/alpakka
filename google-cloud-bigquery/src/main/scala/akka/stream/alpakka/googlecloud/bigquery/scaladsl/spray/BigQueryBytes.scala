/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import akka.util.ByteString
import spray.json.JsString
import java.nio.charset.StandardCharsets.US_ASCII

import scala.util.Try

private[spray] object BigQueryBytes {

  def unapply(bytes: JsString): Option[ByteString] = Try(ByteString(bytes.value, US_ASCII).decodeBase64).toOption

}
