/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.druid

import com.metamx.tranquility.config.{DataSourceConfig, PropertiesBasedConfig}
import com.metamx.tranquility.partition.Partitioner
import com.metamx.tranquility.typeclass.{ObjectWriter, Timestamper}

case class TranquilizerSettings[T](
    dataSourceConfig: DataSourceConfig[PropertiesBasedConfig],
    timestamper: Timestamper[T],
    objectWriter: ObjectWriter[T],
    partitioner: Partitioner[T],
    parallelism: Int = 1,
    dispatcher: String = ""
)
