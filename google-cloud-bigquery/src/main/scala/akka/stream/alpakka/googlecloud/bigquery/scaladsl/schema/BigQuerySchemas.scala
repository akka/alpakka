/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.scaladsl.schema

/**
 * Provides all the predefined BigQuery schemas.
 */
trait BigQuerySchemas
    extends BasicSchemas
    with StandardSchemas
    with CollectionSchemas
    with ProductSchemas
    with JavaTimeSchemas

object BigQuerySchemas extends BigQuerySchemas
