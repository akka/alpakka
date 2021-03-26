package akka.stream.alpakka.googlecloud.bigquery.storage

import scala.collection.mutable

object BigQueryRecord {

  def apply(): BigQueryRecord = new BigQueryRecord()

}

class BigQueryRecord {

  var map = mutable.Map.empty[String, Object]

  def put(column: String, value: Object): Unit = map.put(column, value)

  def get(column: String): Object = map.get(column)

}
