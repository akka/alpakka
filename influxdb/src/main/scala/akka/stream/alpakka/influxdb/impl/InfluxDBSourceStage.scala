package akka.stream.alpakka.influxdb.impl

import akka.annotation.InternalApi
import akka.stream.stage._
import akka.stream.{ActorMaterializer, Attributes, Outlet, SourceShape}
import org.influxdb.InfluxDB
import org.influxdb.dto.{Query, QueryResult}
import scala.collection.JavaConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] trait MessageReader[T] {
  def convert(json: QueryResult): T
}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] trait RowReader[T] {
  def convert(columns: java.util.List[String],row: java.util.List[AnyRef]): T
}


/**
  * INTERNAL API
  */
@InternalApi
private[influxdb] final class InfluxDBSourceStage[T](query: Query,
                                                               client: InfluxDB,
                                                               reader: RowReader[T])
    extends GraphStage[SourceShape[T]] {

  val out: Outlet[T] = Outlet("InfluxDBSourceStage.out")
  override val shape: SourceShape[T] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new InfluxDBSourceLogic[T](query,client, out,shape)

}

/**
  * INTERNAL API
  */
private[influxdb] final class InfluxDBSourceLogic[T](query: Query,
                                                     client: InfluxDB,
                                                     outlet: Outlet[T],
                                                     shape: SourceShape[T],
                                                     reader: RowReader[T])
  extends GraphStageLogic(shape)
    with OutHandler {

  private var columns: java.util.List[String] = null;
  private var rows : Iterator[java.util.List[AnyRef]] = Iterator()

  override def preStart(): Unit = {
    val results = client.query(query)
    columns = results.getResults.get(0).getSeries().get(0).getColumns;
    rows = results.getResults.get(0).getSeries.get(0).getValues.asScala.toIterator
  }

  setHandler(outlet,this)

  override def onPull(): Unit = {
    if(rows.hasNext) {
      val row = rows.next();
      val measure = reader.convert(columns,row)
      emit(outlet, measure)
    } else {
      completeStage()
    }
  }
}

