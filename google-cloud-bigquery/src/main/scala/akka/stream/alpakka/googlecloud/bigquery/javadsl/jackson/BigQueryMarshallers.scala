/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl.jackson

import akka.http.javadsl.marshallers.jackson.Jackson
import akka.http.javadsl.marshalling.Marshaller
import akka.http.javadsl.model.{HttpEntity, MediaTypes, RequestEntity}
import akka.http.javadsl.unmarshalling.Unmarshaller
import akka.stream.alpakka.googlecloud.bigquery.model.QueryJsonProtocol.QueryResponse
import akka.stream.alpakka.googlecloud.bigquery.model.TableDataJsonProtocol.{
  TableDataInsertAllRequest,
  TableDataListResponse
}
import com.fasterxml.jackson.databind.{JavaType, MapperFeature, ObjectMapper}

import java.io.IOException

object BigQueryMarshallers {

  private val defaultObjectMapper = new ObjectMapper().enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)

  def tableDataListResponseUnmarshaller[T](`type`: Class[T]): Unmarshaller[HttpEntity, TableDataListResponse[T]] =
    unmarshaller(defaultObjectMapper.getTypeFactory.constructParametricType(classOf[TableDataListResponse[T]], `type`))

  def tableDataListResponseUnmarshaller[T](mapper: ObjectMapper,
                                           `type`: Class[T]): Unmarshaller[HttpEntity, TableDataListResponse[T]] =
    unmarshaller(mapper, mapper.getTypeFactory.constructParametricType(classOf[TableDataListResponse[T]], `type`))

  def tableDataListRequestMarshaller[T](): Marshaller[TableDataInsertAllRequest[T], RequestEntity] =
    Jackson.marshaller[TableDataInsertAllRequest[T]]()

  def tableDataListRequestMarshaller[T](mapper: ObjectMapper): Marshaller[TableDataInsertAllRequest[T], RequestEntity] =
    Jackson.marshaller[TableDataInsertAllRequest[T]](mapper)

  def queryResponseUnmarshaller[T](`type`: Class[T]): Unmarshaller[HttpEntity, QueryResponse[T]] =
    unmarshaller(defaultObjectMapper.getTypeFactory.constructParametricType(classOf[QueryResponse[T]], `type`))

  def queryResponseUnmarshaller[T](mapper: ObjectMapper, `type`: Class[T]): Unmarshaller[HttpEntity, QueryResponse[T]] =
    unmarshaller(mapper, mapper.getTypeFactory.constructParametricType(classOf[QueryResponse[T]], `type`))

  def unmarshaller[T](expectedType: JavaType): Unmarshaller[HttpEntity, T] =
    unmarshaller(defaultObjectMapper, expectedType)

  def unmarshaller[T](mapper: ObjectMapper, expectedType: JavaType): Unmarshaller[HttpEntity, T] =
    Unmarshaller
      .forMediaType(MediaTypes.APPLICATION_JSON, Unmarshaller.entityToString)
      .thenApply(fromJSON(mapper, _, expectedType))

  private def fromJSON[T](mapper: ObjectMapper, json: String, expectedType: JavaType) =
    try mapper.readerFor(expectedType).readValue[T](json)
    catch {
      case e: IOException =>
        throw new IllegalArgumentException("Cannot unmarshal JSON as " + expectedType.getTypeName, e)
    }
}
