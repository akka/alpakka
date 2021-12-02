/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.googlecloud.bigquery.javadsl.jackson

import akka.http.javadsl.marshallers.jackson.Jackson
import akka.http.javadsl.marshalling.Marshaller
import akka.http.javadsl.model.{HttpEntity, MediaTypes, RequestEntity}
import akka.http.javadsl.unmarshalling.Unmarshaller
import akka.stream.alpakka.googlecloud.bigquery.model.QueryResponse
import akka.stream.alpakka.googlecloud.bigquery.model.{TableDataInsertAllRequest, TableDataListResponse}
import com.fasterxml.jackson.databind.{JavaType, MapperFeature, ObjectMapper}

import java.io.IOException

object BigQueryMarshallers {

  private val defaultObjectMapper = new ObjectMapper().enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY)

  /**
   * [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataListResponse]]
   *
   * @param `type` the data model for each row
   * @tparam T the data model for each row
   */
  def tableDataListResponseUnmarshaller[T](`type`: Class[T]): Unmarshaller[HttpEntity, TableDataListResponse[T]] =
    unmarshaller(defaultObjectMapper.getTypeFactory.constructParametricType(classOf[TableDataListResponse[T]], `type`))

  /**
   * [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataListResponse]]
   *
   * @param mapper an [[ObjectMapper]]
   * @param `type` the data model for each row
   * @tparam T the data model for each row
   */
  def tableDataListResponseUnmarshaller[T](mapper: ObjectMapper,
                                           `type`: Class[T]): Unmarshaller[HttpEntity, TableDataListResponse[T]] =
    unmarshaller(mapper, mapper.getTypeFactory.constructParametricType(classOf[TableDataListResponse[T]], `type`))

  /**
   * [[akka.http.javadsl.marshalling.Marshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataInsertAllRequest]]
   *
   * @tparam T the data model for each row
   */
  def tableDataInsertAllRequestMarshaller[T](): Marshaller[TableDataInsertAllRequest[T], RequestEntity] =
    Jackson.marshaller[TableDataInsertAllRequest[T]]()

  /**
   * [[akka.http.javadsl.marshalling.Marshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.TableDataInsertAllRequest]]
   *
   * @param mapper an [[ObjectMapper]]
   * @tparam T the data model for each row
   */
  def tableDataInsertAllRequestMarshaller[T](
      mapper: ObjectMapper
  ): Marshaller[TableDataInsertAllRequest[T], RequestEntity] =
    Jackson.marshaller[TableDataInsertAllRequest[T]](mapper)

  /**
   * [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.QueryResponse]]
   *
   * @param `type` the data model for each row
   * @tparam T the data model for each row
   */
  def queryResponseUnmarshaller[T](`type`: Class[T]): Unmarshaller[HttpEntity, QueryResponse[T]] =
    unmarshaller(defaultObjectMapper.getTypeFactory.constructParametricType(classOf[QueryResponse[T]], `type`))

  /**
   * [[akka.http.javadsl.unmarshalling.Unmarshaller]] for [[akka.stream.alpakka.googlecloud.bigquery.model.QueryResponse]]
   *
   * @param mapper an [[ObjectMapper]]
   * @param `type` the data model for each row
   * @tparam T the data model for each row
   */
  def queryResponseUnmarshaller[T](mapper: ObjectMapper, `type`: Class[T]): Unmarshaller[HttpEntity, QueryResponse[T]] =
    unmarshaller(mapper, mapper.getTypeFactory.constructParametricType(classOf[QueryResponse[T]], `type`))

  def unmarshaller[T](expectedType: JavaType): Unmarshaller[HttpEntity, T] =
    unmarshaller(defaultObjectMapper, expectedType)

  def unmarshaller[T](mapper: ObjectMapper, expectedType: JavaType): Unmarshaller[HttpEntity, T] =
    Unmarshaller
      .forMediaType(MediaTypes.APPLICATION_JSON, Unmarshaller.entityToString)
      .thenApply(fromJson(mapper, _, expectedType))

  private def fromJson[T](mapper: ObjectMapper, json: String, expectedType: JavaType) =
    try mapper.readerFor(expectedType).readValue[T](json)
    catch {
      case e: IOException =>
        throw new IllegalArgumentException("Cannot unmarshal JSON as " + expectedType.getTypeName, e)
    }
}
