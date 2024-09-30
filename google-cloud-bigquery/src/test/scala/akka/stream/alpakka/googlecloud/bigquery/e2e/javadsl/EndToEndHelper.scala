/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e.javadsl

import akka.stream.alpakka.googlecloud.bigquery.e2e.scaladsl;
import scala.jdk.CollectionConverters._

abstract class EndToEndHelper extends scaladsl.EndToEndHelper {

  def getDatasetId = datasetId
  def getTableId = tableId
  def getRows = rows.asJava

}
