/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.JsString

import scala.util.Try

private[spray] object BigQueryNumber {

  def unapply(n: JsString): Option[BigDecimal] = Try(BigDecimal(n.value)).toOption

}
