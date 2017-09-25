/*
 * Copyright (C) 2016-2017 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.cassandra.scaladsl

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.TestSubscriber
import akka.stream.testkit.scaladsl.TestSink
import com.datastax.driver.core.querybuilder.QueryBuilder.{gt, select}
import com.datastax.driver.core.querybuilder.{QueryBuilder, Select}
import com.datastax.driver.core.{Cluster, Row, Session}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, MustMatchers, WordSpec}

import scala.concurrent.duration._

/**
 * All the tests must be run with a local Cassandra running on default port 9042.
 */
class CassandraRereadableSourceStageTest
    extends WordSpec
    with ScalaFutures
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with MustMatchers {

  private val pollInterval: FiniteDuration = 20.seconds
  private val keyspace = "akka_stream_scala_test"

  private val tableName = "simple_records"
  private type KEY = Long

  private object columns {
    val createdAt = "create_at"
    val key = "key"
    val value = "value"
  }

  //#init-mat
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: ActorMaterializer = ActorMaterializer()
  //#init-mat

  private val cluster: Cluster = Cluster.builder.addContactPoint("127.0.0.1").withPort(9042).build
  //#init-session
  implicit lazy val session: Session = cluster.connect(keyspace)

  //#init-session

  private case class RecordRow(key: UUID, value: String, createAt: Long)

  "CassandraRereadableSourceStageTest" should {
    "createLogic" in {

      val waitingDuration: FiniteDuration = pollInterval

      val fetchSize = 25
      val batchSize = 100

      val key = new UUID(0, 0)
      val mockedRecordRow0 = RecordRow(key, "foo", 0)
      val mockedRecordRow1 = mockedRecordRow0.copy(createAt = 1)
      val mockedRecordRow2 = mockedRecordRow0.copy(createAt = 2)
      val mockedRecordRow3 = mockedRecordRow0.copy(createAt = 3)
      val mockedRecordRow4 = mockedRecordRow0.copy(createAt = 4)
      val mockedRecordRow5 = mockedRecordRow0.copy(createAt = 5)

      insertSimpleRecordRow(mockedRecordRow0)

      val createSelectStatement = { afterId: KEY =>
        generateStatement(key, afterId)
      }

      val source = CassandraSource.rereadable(
        Long.MinValue,
        createSelectStatement,
        _.getLong(columns.createdAt),
        fetchSize,
        batchSize,
        pollInterval
      )

      val probe: TestSubscriber.Probe[RecordRow] =
        source.map(mapSimpleRecordRow).toMat(TestSink.probe)(Keep.right).run()

      def requestAndExpect(simpleRecordRow: RecordRow) =
        probe.request(1).expectNext(waitingDuration, simpleRecordRow)

      def expectNoMsg = probe.expectNoMsg(5.second)

      requestAndExpect(mockedRecordRow0)
      expectNoMsg

      insertSimpleRecordRow(mockedRecordRow1)
      insertSimpleRecordRow(mockedRecordRow2)
      insertSimpleRecordRow(mockedRecordRow3)
      insertSimpleRecordRow(mockedRecordRow4)

      requestAndExpect(mockedRecordRow1)
      requestAndExpect(mockedRecordRow2)

      insertSimpleRecordRow(mockedRecordRow5)
      requestAndExpect(mockedRecordRow3)
      requestAndExpect(mockedRecordRow4)
      requestAndExpect(mockedRecordRow5)

      expectNoMsg
    }

  }

  private def generateStatement(key: UUID, createAt: Long): Select.Where =
    select()
      .from(tableName)
      .where(gt(columns.createdAt, createAt))
      .and(QueryBuilder.eq(columns.key, key))

  private def insertSimpleRecordRow(simpleRecordRow: RecordRow): Unit =
    session.execute(
      s"""
         |INSERT INTO $tableName(${columns.key}, ${columns.createdAt}, ${columns.value})
         |VALUES (${simpleRecordRow.key}, ${simpleRecordRow.createAt}, '${simpleRecordRow.value}');
       """.stripMargin
    )

  private def mapSimpleRecordRow(row: Row): RecordRow =
    RecordRow(
      row.getUUID(columns.key),
      row.getString(columns.value),
      row.getLong(columns.createdAt)
    )

  override protected def beforeEach: Unit = {
    val session = cluster.connect() //TODO: refine
    session.execute(
      s"""
        |CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = {
        |  'class': 'SimpleStrategy',
        |  'replication_factor': '1'
        |};
      """.stripMargin
    )
    session.execute(
      s"""
         |CREATE TABLE IF NOT EXISTS $keyspace.$tableName(
         |${columns.key} uuid,
         |${columns.value} text,
         |${columns.createdAt} bigint,
         |PRIMARY KEY(${columns.key}, ${columns.createdAt})) WITH CLUSTERING ORDER BY (${columns.createdAt} ASC);
      """.stripMargin
    )
    session.close()
  }

  override protected def afterEach: Unit = {
    session.execute(s"DROP TABLE IF EXISTS $keyspace.$tableName;")
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
  }
}
