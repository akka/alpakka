/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl

import akka.stream.alpakka.googlecloud.bigquery.scaladsl

import java.util
import scala.compat.java8.OptionConverters._

trait Paginated {
  def getPageToken: util.Optional[String]
}

private[javadsl] object Paginated {
  implicit object paginatedIsPaginated extends scaladsl.Paginated[Paginated] {
    override def pageToken(t: Paginated): Option[String] = t.getPageToken.asScala
  }
}
