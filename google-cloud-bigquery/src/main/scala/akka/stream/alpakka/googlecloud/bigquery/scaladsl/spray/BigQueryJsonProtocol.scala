/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{AdditionalFormats, ProductFormats}

/**
 * Provides all the predefined BigQueryJsonFormats for rows and cells in BigQuery tables.
 */
trait BigQueryJsonProtocol
    extends BigQueryBasicFormats
    with BigQueryStandardFormats
    with BigQueryCollectionFormats
    with ProductFormats
    with BigQueryProductFormats
    with AdditionalFormats
    with BigQueryJavaTimeFormats

object BigQueryJsonProtocol extends BigQueryJsonProtocol
