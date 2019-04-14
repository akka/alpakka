package akka.stream.alpakka.influxdb.javadsl

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.javadsl.Source
import org.influxdb.InfluxDB
import org.influxdb.dto.Query
import akka.stream.alpakka.influxdb.impl.InfluxDBSourceStage
import akka.stream.alpakka.influxdb.impl
import org.influxdb.impl.InfluxDBResultMapperHelper

/**
 * Java API to create InfluxDB sources.
 */
object InfluxDBSource {

  /**
   * Java API: creates a [[InfluxDBSourceStage]] from a given statement.
   */
  def create[T](query: Query, influxDB: InfluxDB,clazz: Class[T]): Source[T, NotUsed] =
    akka.stream.javadsl.Source.fromGraph(new InfluxDBSourceStage[T](query, influxDB,new RowReaderImpl[T](clazz)))

  private final class RowReaderImpl[T](clazz: Class[T]) extends impl.RowReader[T] {

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
