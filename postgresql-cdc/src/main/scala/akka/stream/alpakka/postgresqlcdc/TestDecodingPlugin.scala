/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.time._

import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import fastparse.all._

import scala.collection.mutable.ArrayBuffer

/**
 * INTERNAL API
 */
@InternalApi private[postgresqlcdc] object TestDecodingPlugin {

  import PostgreSQL._

  /*

  What we get from PostgreSQL is something like the following:

  location  | xid |                     data
 -----------+-----+-----------------------------------------------
  0/16E0478 | 689 | BEGIN 689
  0/16E0478 | 689 | table public.data: INSERT: id[integer]:1 data[text]:'1'
  0/16E0580 | 689 | table public.data: INSERT: id[integer]:2 data[text]:'2'
  0/16E0650 | 689 | COMMIT 689

  The grammar below is for parsing what is inside the rows of the data column.

   */

  case class BeginStatement(number: Long)

  case class CommitStatement(number: Long, zonedDateTime: ZonedDateTime)

  case class Field(columnName: String, columnType: String, value: String)

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

  val date: Parser[LocalDate] = P(fourDigitInt ~ "-" ~ twoDigitInt ~ "-" ~ twoDigitInt).map { t ⇒
    LocalDate.of(t._1, t._2, t._3)
  }

  val time: Parser[(LocalTime, ZoneId)] =
    P(twoDigitInt ~ ":" ~ twoDigitInt ~ ":" ~ twoDigitInt ~ "." ~ numberInt ~ (P("+" | "-") ~ digit.rep(min = 1)).!)
      .map { t ⇒
        LocalTime.of(t._1, t._2, t._3, t._4) → ZoneOffset.of(t._5)
      }

  val timestamp: Parser[ZonedDateTime] = P(date ~ space ~ time).map(s ⇒ ZonedDateTime.of(s._1, s._2._1, s._2._2))

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

  // captures my_column_name or something_else
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

  // matches a[integer]:1 b[integer]:1 c[integer]:3
  val data: Parser[List[Field]] = P(identifier ~ typeDeclaration ~ ":" ~ value)
    .rep(min = 1, sep = space)
    .map(s ⇒ s.map(f ⇒ Field(f._1, f._2, f._3)))
    .map(_.toList)

  // when we have both the old version and the new version of the row
  val both: Parser[(List[Field], List[Field])] = P(
    "old-key:" ~ space ~ data ~ space ~ "new-tuple:" ~ space ~ data
  )

  // when we have only the new version of the row
  val latest: Parser[(List[Field], List[Field])] = data.map(v ⇒ (List.empty[Field], v))

  // note: we need to wrap the function in an abstract class to get rid of a type erasure problem
  abstract class ChangeBuilder extends (((String, Long)) ⇒ Change) // (location: String, transactionId: Long) ⇒ Change

  val changeStatement: Parser[ChangeBuilder] = {
    val getData: List[Field] ⇒ Map[String, String] = fieldList ⇒ fieldList.map(f ⇒ f.columnName → f.value).toMap
    val getSchema: List[Field] ⇒ Map[String, String] = fieldList ⇒ fieldList.map(f ⇒ f.columnName → f.columnType).toMap
    P(s"table" ~ space ~ identifier ~ "." ~ identifier ~ ":" ~ space ~ changeType ~ ":" ~ space ~ P(latest | both))
      .map { m ⇒
        {
          val schemaName = m._1
          val tableName = m._2
          val fields = m._4
          new ChangeBuilder {
            override def apply(info: (String, Long)): Change =
              m._3 match {
                case "INSERT" ⇒
                  RowInserted(schemaName = schemaName,
                              tableName = tableName,
                              logSeqNum = info._1,
                              transactionId = info._2,
                              data = getData(fields._2),
                              schema = getSchema(fields._2))
                case "DELETE" ⇒
                  RowDeleted(schemaName = schemaName,
                             tableName = tableName,
                             commitLogSeqNum = info._1,
                             transactionId = info._2,
                             data = getData(fields._2),
                             schema = getSchema(fields._2))
                case "UPDATE" ⇒
                  RowUpdated(
                    schemaName = schemaName,
                    tableName = tableName,
                    commitLogSeqNum = info._1,
                    transactionId = info._2,
                    dataNew = getData(fields._2),
                    dataOld = getData(fields._1),
                    schemaNew = getSchema(fields._2),
                    schemaOld = getSchema(fields._1)
                  )
              }
          }
        }
      }
  }

  val statement = P(changeStatement | begin | commit)

  def getColsToIgnoreForTable(tableName: String, colsToIgnorePerTable: Map[String, List[String]]): Set[String] = {
    val colsToAlwaysIgnore: Set[String] = colsToIgnorePerTable.filter { case (k, _) ⇒ k == "*" }.values.flatten.toSet
    colsToAlwaysIgnore ++ colsToIgnorePerTable.get(tableName).map(_.toSet).getOrElse(Set.empty)
  }

  def slotChangesToChangeSet(
      transactionId: Long,
      slotChanges: List[SlotChange],
      colsToIgnorePerTable: Map[String, List[String]]
  )(implicit log: LoggingAdapter): ChangeSet = {

    val ignoreTables: Set[String] = colsToIgnorePerTable.filter { case (_, v) ⇒ v == "*" :: Nil }.keys.toSet

    val result = ArrayBuffer[Change]()
    var instant: Instant = null
    var commitLogSeqNum: String = null

    // the last item is the "COMMIT _ (at _)"
    (slotChanges.last, commit.parse(slotChanges.last.data)) match {
      case (s, Parsed.Success(CommitStatement(_, t: ZonedDateTime), _)) ⇒
        instant = t.toInstant
        commitLogSeqNum = s.location
      case (s, f: Parsed.Failure) ⇒
        log.error("failure {} when parsing {}", f.toString(), s.data)
    }

    // we drop the first item and the last item since the first one is just the "BEGIN _" and the last one is the "COMMIT _ (at _)"
    slotChanges.drop(1).dropRight(1).map(s ⇒ (s, changeStatement.parse(s.data))).foreach {

      case (s, Parsed.Success(changeBuilder: ChangeBuilder, _)) ⇒
        val change = changeBuilder((commitLogSeqNum, transactionId))
        if (!ignoreTables.contains(change.tableName)) {
          val hidden: String ⇒ Boolean =
            f ⇒ getColsToIgnoreForTable(change.tableName, colsToIgnorePerTable).contains(f)
          result += (change match {
            case insert: RowInserted ⇒
              insert.copy(data = insert.data.filterKeys(!hidden(_)))
            case delete: RowDeleted ⇒
              delete.copy(data = delete.data.filterKeys(!hidden(_)))
            case update: RowUpdated ⇒
              update.copy(dataNew = update.dataNew.filterKeys(!hidden(_)),
                          dataOld = update.dataOld.filterKeys(!hidden(_)))
          })
        }

      case (s, f: Parsed.Failure) ⇒
        log.error("failure {} when parsing {}", f.toString(), s.data)

    }

    ChangeSet(transactionId, slotChanges.last.location, instant, result.toList)
  }

  def transformSlotChanges(slotChanges: List[SlotChange], colsToIgnorePerTable: Map[String, List[String]])(
      implicit log: LoggingAdapter
  ): List[ChangeSet] =
    slotChanges
      .groupBy(_.transactionId)
      .map {
        case (transactionId: Long, changesByTransactionId: List[SlotChange]) ⇒
          slotChangesToChangeSet(transactionId, changesByTransactionId, colsToIgnorePerTable)
      }
      .filter(_.changes.nonEmpty)
      .toList
      .sortBy(_.transactionId)

}
