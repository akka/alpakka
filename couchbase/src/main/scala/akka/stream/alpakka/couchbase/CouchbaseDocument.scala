package akka.stream.alpakka.couchbase

class CouchbaseDocument[T](id: String, document: T) {
  def getId: String = id;
  def getDocument: T = document;
}
