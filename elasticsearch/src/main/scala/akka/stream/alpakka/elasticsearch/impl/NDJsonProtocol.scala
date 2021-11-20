/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.elasticsearch.impl

import akka.http.scaladsl.model.{ContentType, HttpCharsets, MediaType}

object NDJsonProtocol {

  val `application/x-ndjson`: MediaType.WithFixedCharset =
    MediaType.applicationWithFixedCharset("x-ndjson", HttpCharsets.`UTF-8`)
  val ndJsonContentType: ContentType.WithFixedCharset = ContentType.apply(`application/x-ndjson`)
}
