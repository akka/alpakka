package akka.stream.alpakka.hbase.internal

import org.apache.hadoop.hbase.client.Put

sealed trait HBasePutCommand[T]

case class SimplePutCommand[T](put: Put) extends HBasePutCommand[T]

case class MultiPutCommand[T](puts: java.util.List[Put]) extends HBasePutCommand[T]
