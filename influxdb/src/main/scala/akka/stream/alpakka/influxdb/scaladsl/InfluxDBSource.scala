package akka.stream.alpakka.influxdb.scaladsl

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Source
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query, QueryResult}
import akka.stream.alpakka.influxdb.impl
import akka.stream.alpakka.influxdb.impl.InfluxDBSourceStage
import org.influxdb.impl.{InfluxDBResultMapper, InfluxDBResultMapperHelper}

import scala.collection.JavaConverters._

/**
 * Scala API to create InfluxDB sources.
 */
object InfluxDBSource {

  /**
   * Scala API: creates a [[InfluxDBSourceStage]] from a given statement.
   */
  def apply[T](query: Query)(implicit influxDB: InfluxDB): Source[T, NotUsed] =
    Source.fromGraph(
      new impl.InfluxDBSourceStage[T](query,influxDB, new RowReaderImpl[T])
    )

  private final class QueryResultReader[T] extends impl.MessageReader[List[T]] {
    override def convert(queryResult: QueryResult): List[T] = {
      val influxDBResultMapper = new InfluxDBResultMapper();
      influxDBResultMapper.toPOJO(queryResult,classOf[T])
        .asScala
        .toList
    }
  }

  private final class RowReaderImpl[T] extends impl.RowReader[T] {

    val mapperHelper = init()

    def init(): InfluxDBResultMapperHelper = {
      val resultMapperHelper = new InfluxDBResultMapperHelper
      resultMapperHelper.cacheClassFields(classOf[T])
      resultMapperHelper
    }

    override def convert(column: java.util.List[String],row: java.util.List[AnyRef]): T = {
      mapperHelper.parseRowAs(classOf[T],column,row,TimeUnit.MILLISECONDS)
    }
  }

}

