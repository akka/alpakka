/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.Document
import rx.Observable
import rx.lang.scala.JavaConversions.toScalaObservable
import scala.collection.immutable.Seq
import scala.reflect.{classTag, ClassTag}

/**
 * Internal API
 */
@InternalApi
private[couchbase] object CouchbaseSourceImpl {

  private[couchbase] def fromSingleId[T <: Document[_]](id: String,
                                                        couchbaseBucket: Bucket,
                                                        target: Class[T]): Observable[T] = {
    implicit val tag: ClassTag[T] = scala.reflect.ClassTag.apply(target)
    couchbaseBucket.async().get(id, classTag[T].runtimeClass.asInstanceOf[target.type]).single()
  }

  private[couchbase] def fromBulkIds[T <: Document[_]](ids: Seq[String],
                                                       couchbaseBucket: Bucket,
                                                       target: Class[T]): rx.lang.scala.Observable[T] = {
    implicit val tag: ClassTag[T] = scala.reflect.ClassTag.apply(target)
    rx.lang.scala.Observable
      .from(ids)
      .flatMap(
        id => toScalaObservable(couchbaseBucket.async().get(id, classTag[T].runtimeClass.asInstanceOf[target.type]))
      )
  }

}
