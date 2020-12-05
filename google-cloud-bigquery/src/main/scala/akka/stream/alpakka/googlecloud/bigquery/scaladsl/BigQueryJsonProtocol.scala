/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl

import spray.json.{AdditionalFormats, CollectionFormats, ProductFormats, StandardFormats}

trait BigQueryJsonProtocol
    extends BigQueryBasicFormats
    with StandardFormats
    with CollectionFormats
    with ProductFormats
    with BigQueryProductFormats
    with AdditionalFormats

object BigQueryJsonProtocol extends BigQueryJsonProtocol
