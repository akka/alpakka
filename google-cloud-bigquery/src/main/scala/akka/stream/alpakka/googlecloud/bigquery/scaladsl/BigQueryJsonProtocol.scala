/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import spray.json.{AdditionalFormats, ProductFormats, StandardFormats}

trait BigQueryJsonProtocol
    extends BigQueryBasicFormats
    with StandardFormats
    with BigQueryCollectionFormats
    with ProductFormats
    with BigQueryProductFormats
    with AdditionalFormats

object BigQueryJsonProtocol extends BigQueryJsonProtocol
