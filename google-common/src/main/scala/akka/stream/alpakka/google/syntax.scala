/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google

import akka.http.scaladsl.model.Uri.Query

object syntax {

  implicit final class QueryPrependOption(val query: Query) extends AnyVal {
    def ?+:(kv: (String, Option[Any])): Query = kv._2.fold(query)(v => Query.Cons(kv._1, v.toString, query))
  }

}
