/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{AdditionalFormats, StandardFormats}

/**
 * Provides the BigQueryJsonFormats for the non-collection standard types.
 */
trait BigQueryStandardFormats extends StandardFormats { this: AdditionalFormats =>

  implicit def bigQueryOptionFormat[T: BigQueryJsonFormat]: BigQueryJsonFormat[Option[T]] =
    new OptionFormat[T] with BigQueryJsonFormat[Option[T]]

}
