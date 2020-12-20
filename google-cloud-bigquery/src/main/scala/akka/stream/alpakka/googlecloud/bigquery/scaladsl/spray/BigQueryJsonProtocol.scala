/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.spray

import spray.json.{AdditionalFormats, ProductFormats}

trait BigQueryJsonProtocol
    extends BigQueryBasicFormats
    with BigQueryStandardFormats
    with BigQueryCollectionFormats
    with ProductFormats
    with BigQueryProductFormats
    with AdditionalFormats

object BigQueryJsonProtocol extends BigQueryJsonProtocol
