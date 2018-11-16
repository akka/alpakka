/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.google.cloud.bigquery.impl.pagetoken

import akka.NotUsed
import akka.stream.alpakka.google.cloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.scaladsl.Flow

object EndOfStreamDetector {

  def apply(): Flow[(Boolean, PagingInfo), (Boolean, PagingInfo), NotUsed] =
    Flow[(Boolean, PagingInfo)].takeWhile {
      case (retry, pagingInfo) => retry || pagingInfo.pageToken.isDefined
    }

}
