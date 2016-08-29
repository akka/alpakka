This project provides a way to transform a Cassandra's ResultSet into a Akka Stream's Source.

Example:

```scala
import akka.actor.ActorSystem
import akka.actor.stream.ActorMaterializer
import akka.stream.contrib.cassandra.CassandraSource
import com.datastax.driver.core.{SimpleStatement, Cluster}

implicit val system = ActorSystem()
implicit val mat = ActorMaterializer()

val cluster = Cluster.builder.addContactPoint("127.0.0.1").withPort(9042).build
implicit val session = cluster.connect()

val stmt = new SimpleStatement("SELECT * FROM akka_stream_test.test")

val cassSource: Source[Row, NotUsed] = CassandraSource(stmt)
```