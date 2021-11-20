/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{AdditionalFormats, CollectionFormats, ProductFormats, StandardFormats}

/**
 * Provides all the predefined JsonFormats for the BigQuery REST API.
 */
trait BigQueryRestJsonProtocol
    extends BigQueryRestBasicFormats
    with StandardFormats
    with CollectionFormats
    with ProductFormats
    with AdditionalFormats

object BigQueryRestJsonProtocol extends BigQueryRestJsonProtocol
