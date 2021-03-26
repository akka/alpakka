/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.e2e.javadsl

import akka.stream.alpakka.googlecloud.bigquery.e2e.scaladsl;
import scala.collection.JavaConverters._

abstract class EndToEndHelper extends scaladsl.EndToEndHelper {

  def getDatasetId = datasetId
  def getTableId = tableId
  def getRows = rows.asJava

}
