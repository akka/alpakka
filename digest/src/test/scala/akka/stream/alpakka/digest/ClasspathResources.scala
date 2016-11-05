/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.digest

import java.io.InputStream

import akka.stream.IOResult
import akka.stream.scaladsl.{ Source, StreamConverters }
import akka.util.ByteString

import scala.concurrent.Future
import scala.io.{ Source => ScalaIOSource }

trait ClasspathResources {
  def withInputStream[T](fileName: String)(f: InputStream => T): T = {
    val is: InputStream = fromClasspathAsStream(fileName)
    try f(is) finally is.close()
  }

  def withInputStreamAsText[T](fileName: String)(f: String => T): T =
    f(fromClasspathAsString(fileName))

  def withByteStringSource[T](fileName: String)(f: Source[ByteString, Future[IOResult]] => T): T =
    withInputStream(fileName) { inputStream =>
      f(StreamConverters.fromInputStream(() => inputStream))
    }

  def streamToString(is: InputStream): String =
    ScalaIOSource.fromInputStream(is).mkString

  def fromClasspathAsString(fileName: String): String =
    streamToString(fromClasspathAsStream(fileName))

  def fromClasspathAsStream(fileName: String): InputStream =
    getClass.getClassLoader.getResourceAsStream(fileName)
}
