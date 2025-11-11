/*
 * Copyright (C) since 2016 Lightbend Inc. <https://akka.io>
 */

package akka.stream.alpakka.couchbase

final class CouchbaseDocument[T](val id: String, val document: T) {
  def getId: String = id;
  def getDocument: T = document;
}
