/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.s3.scaladsl

import java.time.Instant

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{`Cache-Control`, `Content-Length`, `Content-Type`, `Last-Modified`, ETag}
import akka.stream.alpakka.s3.impl._
