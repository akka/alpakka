/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

/**
 * Provides all the predefined BigQuery schemas.
 */
trait BigQuerySchemas extends BasicSchemas with StandardSchemas with CollectionSchemas with ProductSchemas

object BigQuerySchemas extends BigQuerySchemas
