/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.impl

import akka.annotation.InternalApi
import akka.stream.alpakka.typesense.IndexDocumentResult

@InternalApi private[typesense] object CollectionResponses {
  final case class IndexManyDocumentsResponse(success: Boolean, error: Option[String], document: Option[String]) {
    def asResult: IndexDocumentResult =
      if (success) IndexDocumentResult.IndexSuccess()
      else IndexDocumentResult.IndexFailure(error.getOrElse(""), document.getOrElse(""))
  }
}
