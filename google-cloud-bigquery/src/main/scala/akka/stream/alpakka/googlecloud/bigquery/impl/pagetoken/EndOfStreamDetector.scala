/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.impl.pagetoken

import akka.NotUsed
import akka.annotation.InternalApi
import akka.stream.alpakka.googlecloud.bigquery.impl.parser.Parser.PagingInfo
import akka.stream.scaladsl.Flow

@InternalApi
private[impl] object EndOfStreamDetector {

  def apply(): Flow[(Boolean, PagingInfo), (Boolean, PagingInfo), NotUsed] =
    Flow[(Boolean, PagingInfo)].takeWhile {
      case (retry, pagingInfo) => retry || pagingInfo.pageToken.isDefined
    }

}
