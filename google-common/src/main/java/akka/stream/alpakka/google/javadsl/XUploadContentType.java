/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.google.javadsl;

import akka.http.javadsl.model.ContentType;
import akka.stream.alpakka.google.scaladsl.X$minusUpload$minusContent$minusType$;

/** Models the `X-Upload-Content-Type` header for resumable uploads. */
public interface XUploadContentType {

  ContentType getContentType();

  static XUploadContentType create(ContentType contentType) {
    return X$minusUpload$minusContent$minusType$.MODULE$.apply(
        (akka.http.scaladsl.model.ContentType) contentType);
  }
}
