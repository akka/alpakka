/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{AdditionalFormats, StandardFormats}

trait BigQueryStandardFormats extends StandardFormats { this: AdditionalFormats =>

  implicit def bigQueryOptionFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[Option[T]] =
    new OptionFormat[T] with BigQueryJsonFormat[Option[T]]

}
