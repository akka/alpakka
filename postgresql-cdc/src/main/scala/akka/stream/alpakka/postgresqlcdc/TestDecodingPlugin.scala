/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.time._

import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.stream.alpakka.postgresqlcdc.PostgreSQL.SlotChange
import fastparse.all._

import scala.collection.mutable.ArrayBuffer

@InternalApi private[postgresqlcdc] object TestDecodingPlugin {

  //
  // We need to parse a log statement such as the following:
  //
  // BEGIN 2380
  // table public.table_name: INSERT: id[integer]:3 data[text]:'3'
  // COMMIT 2380 (at 2018-04-09 17:56:36.730413+00)
  //

  case class BeginStatement(number: Long)

  case class CommitStatement(number: Long, zonedDateTime: ZonedDateTime)

  val singleQuote: Parser[Unit] = P("'")

  val doubleQuote: Parser[Unit] = P("\"")

  val digit: Parser[Unit] = P(CharIn(strings = '0' to '9'))

  val lowerCaseLetter: Parser[Unit] = P(CharIn(strings = 'a' to 'z'))

  val upperCaseLetter: Parser[Unit] = P(CharIn(strings = 'A' to 'Z'))

  val letter: Parser[Unit] = P(lowerCaseLetter | upperCaseLetter)

  val numberLong: Parser[Long] = P(digit.rep(min = 1).!.map(_.toLong))

  val numberInt: Parser[Int] = P(digit.rep(min = 1).!.map(_.toInt))

  val space: Parser[Unit] = P(" ")

  val twoDigitInt: Parser[Int] = P(digit.rep(exactly = 2).!.map(_.toInt))

  val fourDigitInt: Parser[Int] = P(digit.rep(exactly = 4).!.map(_.toInt))

  val date: Parser[LocalDate] = P(fourDigitInt ~ "-" ~ twoDigitInt ~ "-" ~ twoDigitInt).map { t =>
    LocalDate.of(t._1, t._2, t._3)
  }

  val time: Parser[(LocalTime, ZoneId)] =
    P(twoDigitInt ~ ":" ~ twoDigitInt ~ ":" ~ twoDigitInt ~ "." ~ numberInt ~ (P("+" | "-") ~ digit.rep(min = 1)).!)
      .map { t =>
        LocalTime.of(t._1, t._2, t._3, t._4) -> ZoneOffset.of(t._5)
      }

  val timestamp: Parser[ZonedDateTime] = P(date ~ space ~ time).map(s => ZonedDateTime.of(s._1, s._2._1, s._2._2))

  val begin: Parser[BeginStatement] = P("BEGIN" ~ space ~ numberLong).map(BeginStatement)

  // matches a commit message like COMMIT 2380 (at 2018-04-09 17:56:36.730413+00)
  // captures the commit number and the time
  val commit: Parser[CommitStatement] =
    P("COMMIT" ~ space ~ numberLong ~ space ~ "(" ~ "at" ~ space ~ timestamp ~ ")").map(CommitStatement.tupled)

  // matches "Scala", "'Scala'" or "The ""Scala"" language", "The \"Scala\" language" etc
  // captures the full string
  val doubleQuotedString: Parser[String] = {
    val twoDoubleQuotes = P("\"\"")
    val escapeDoubleQuote = P("\\\"")
    val escape = P(twoDoubleQuotes | escapeDoubleQuote)
    P(doubleQuote ~ P((!(escape | doubleQuote) ~ AnyChar) | escape).rep ~ doubleQuote).!
  }

  // matches 'Scala', 'The ''Scala'' language' etc.
  // captures what's inside the single quotes (i.e., for 'Scala' it captures Scala)
  val singleQuotedString: Parser[String] = {
    val twoSingleQuoutes = P("''")
    val escapeSingleQuote = P("\\\'")
    val escape = P(twoSingleQuoutes | escapeSingleQuote)
    P(singleQuote ~ P((!(escape | singleQuote) ~ AnyChar) | escape).rep.! ~ singleQuote)
  }

  // captures my_column_name
  val unquotedIdentifier: Parser[String] = P(lowerCaseLetter | digit | "$" | "_").rep(min = 1).!

  // captures my_column_name or "MY_COLUMN_NAME"
  val identifier: Parser[String] = P(unquotedIdentifier | doubleQuotedString)

  // matches [character varying], [integer], [xml], [text[]], [text[][]], [integer[]], [integer[3]], [integer[3][3]] etc.
  // captures what's inside the square brackets
  val typeDeclaration: Parser[String] = {
    val arraySyntax = P("[" ~ digit.rep(min = 0) ~ "]")
    "[" ~ P(space | letter | digit | arraySyntax).rep.! ~ "]"
  }

  // matches 42, 3.14, true, false or 'some string value'
  val value: Parser[String] = P(P(!(space | singleQuote) ~ AnyChar).rep(min = 1).! | singleQuotedString)

  val changeType: Parser[String] = P("INSERT" | "UPDATE" | "DELETE").!

  val data: Parser[List[Field]] = P(identifier ~ typeDeclaration ~ ":" ~ value)
    .rep(min = 1, sep = space)
    .map(s => s.map(f => Field(f._1, f._2, f._3)))
    .map(_.toList)

  // when we have both the old version and the new version of the row
  val both: Parser[(List[Field], List[Field])] = P(
    "old-key:" ~ space ~ data ~ space ~ "new-tuple:" ~ space ~ data
  )

  // when we have only the new version of the row
  val latest: Parser[(List[Field], List[Field])] = data.map(v => (List.empty[Field], v))

  val changeStatement: Parser[Change] =
    P(
      s"table" ~ space ~ identifier ~ "." ~ identifier ~ ":" ~ space ~ changeType ~ ":" ~ space ~ P(latest | both)
    ).map { m =>
      {
        val schemaName = m._1
        val tableName = m._2
        val fields = m._4
        m._3 match {
          case "INSERT" => RowInserted(schemaName, tableName, fields._2)
          case "DELETE" => RowDeleted(schemaName, tableName, fields._2)
          case "UPDATE" => RowUpdated(schemaName, tableName, fields._2, fields._1)
        }
      }
    }

  val statement: P[Object] = P(changeStatement | begin | commit)

  def slotChangesToChangeSet(transactionId: Long,
                             slotChanges: List[SlotChange])(implicit log: LoggingAdapter): ChangeSet = {

    val result = ArrayBuffer[Change]()
    var zonedDateTime: ZonedDateTime = null

    slotChanges.map(s => statement.parse(s.data)).foreach {
      case Parsed.Success(c: BeginStatement, _) => // ignore
      case Parsed.Success(c: Change, _) =>
        result += c
      case Parsed.Success(CommitStatement(_, t: ZonedDateTime), _) => zonedDateTime = t
      case f: Parsed.Failure =>
        log.error("failed to parse item", f.toString())
      case _ => // ignore
    }

    ChangeSet(transactionId, slotChanges.last.location, zonedDateTime, result.toList)
  }

  def filterOutColumns(cs: ChangeSet, ignoreColumnsPerTable: Map[String, List[String]]): ChangeSet = {

    val ignoreTables: Set[String] = ignoreColumnsPerTable.filter { case (_, v) => v == List("*") }.keys.toSet
    val ignoreColumns: Set[String] = ignoreColumnsPerTable.filter { case (k, v) => k == "*" }.values.flatten.toSet

    val result = cs.changes.filter(c => !ignoreTables.contains(c.tableName)).map { change =>
      {
        val toIgnore = ignoreColumns ++ ignoreColumnsPerTable.get(change.tableName).map(_.toSet).getOrElse(Set.empty)
        change match {

          case RowInserted(s, t, fields) =>
            RowInserted(schemaName = s, tableName = t, fields = fields.filter(f => !toIgnore.contains(f.columnName)))

          case RowDeleted(s, t, fields) =>
            RowDeleted(schemaName = s, tableName = t, fields = fields.filter(f => !toIgnore.contains(f.columnName)))

          case RowUpdated(s, t, fieldsNew, fieldsOld) =>
            RowUpdated(
              schemaName = s,
              tableName = t,
              fieldsNew = fieldsNew.filter(f => !toIgnore.contains(f.columnName)),
              fieldsOld = fieldsOld.filter(f => !toIgnore.contains(f.columnName))
            )
        }
      }

    }

    ChangeSet(cs.transactionId, cs.location, cs.zonedDateTime, result)

  }

  def transformSlotChanges(slotChanges: List[SlotChange], ignoreColumnsPerTable: Map[String, List[String]])(
      implicit log: LoggingAdapter
  ): List[ChangeSet] =
    slotChanges
      .groupBy(_.transactionId)
      .map {
        case (transactionId: Long, changesByTransactionId: List[SlotChange]) => {
          val changeSet = slotChangesToChangeSet(transactionId, changesByTransactionId)
          filterOutColumns(changeSet, ignoreColumnsPerTable)
        }
      }
      .filter(_.changes.nonEmpty)
      .toList
      .sortBy(_.transactionId)

}
