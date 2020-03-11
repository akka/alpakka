/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase
import akka.annotation.InternalApi
import com.couchbase.client.java.document.json.JsonObject

/**
 * Describes a Couchbase related failure with an error code.
 */
final class CouchbaseResponseException(msg: String, val code: Option[Int]) extends RuntimeException(msg) {

  override def toString = s"CouchbaseResponseException($msg, $code)"
}

/** INTERNAL API */
@InternalApi
private[akka] object CouchbaseResponseException {
  def apply(json: JsonObject): CouchbaseResponseException =
    new CouchbaseResponseException(
      msg = if (json.containsKey("msg")) json.getString("msg") else "",
      code = if (json.containsKey("code")) Some(json.getInt("code")) else None
    )
}
