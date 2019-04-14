package akka.stream.alpakka.influxdb

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] sealed abstract class OperationType(val command: String) {
  override def toString: String = command
}

/**
 * INTERNAL API
 */
@InternalApi
private[influxdb] object InfuxDBOperationType {
  object CreateDatabase extends OperationType("create_database")
  object CreateRetentionPolicy extends OperationType("create_retention_policy")
  object AlterRetentionPolicy extends OperationType("alter_retention_policy")
  object DropDatabase extends OperationType("drop_database")
  object DropSeries extends OperationType("drop_series")
  object DropRetentionPolicy extends OperationType("drop_retention_policy")
  object DropMeasurement extends OperationType("drop_measurement")
  object DropShard extends OperationType("drop_shard")
  object Delete extends OperationType("delete")
}
