/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase

class CouchbaseDocument[T](id: String, document: T) {
  def getId: String = id;
  def getDocument: T = document;
}
