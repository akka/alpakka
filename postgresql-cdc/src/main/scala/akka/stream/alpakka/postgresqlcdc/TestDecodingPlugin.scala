/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import akka.annotation.InternalApi
import akka.stream.alpakka.postgresqlcdc.PostgreSQL.SlotChange

import scala.util.matching.Regex

@InternalApi private[postgresqlcdc] object TestDecodingPlugin {

  // We need to parse a log statement such as the following:
  //
  // table public.data: INSERT: id[integer]:3 data[text]:'3'
  //
  // Though it's complicated to parse it's not complicated enough to justify the cost of an additional dependency (could
  // have used FastParse or Scala Parser Combinators), hence we use standard library regular expressions.

  val Begin: Regex = "BEGIN (\\d+)".r

  val Commit: Regex = "COMMIT (\\d+) \\(at (\\d{4}-\\d{2}-\\d{2}) (\\d{2}:\\d{2}:\\d{2}\\.\\d+\\+\\d{2})\\)".r
  // matches a commit message like COMMIT 2380 (at 2018-04-09 17:56:36.730413+00)

  val DoubleQuotedString: String = "\"(?:\\\\\"|\"{2}|[^\"])+\""
  // matches "Scala", "'Scala'"
  // or "The ""Scala"" language", "The \"Scala\" language" etc.

  val SingleQuotedString1: String = "'(?:\\\\'|'{2}|[^'])+'"
  // matches 'Scala', 'The ''Scala'' language' etc.

  val SingleQuotedString2: Regex = "'((?:\\\\'|'{2}|[^'])+)'".r

  val UnquotedIdentifier: String = "[^ \"'\\.]+"

  val Identifier: String = s"(?:$UnquotedIdentifier)|(?:$DoubleQuotedString)"

  val FieldIdentifier: String = Identifier

  val TypeDeclaration: String = "(?:[a-zA-Z0-9 ]|\\[[0-9]*\\])+"
  // matches: character varying, integer, xml, text[], text[][], integer[], integer[3], integer[3][3] etc.

  val NonStringValue: String = "[^ \"']+"
  // matches: true, false, 3.14, 42 etc.

  val Value: String = s"(?:$NonStringValue)|(?:$SingleQuotedString1)"
  // matches: true, false, 3.14 or 'Strings can have spaces'

  val ChangeStatement: Regex = {
    val SchemaIdentifier: String = Identifier
    val TableIdentifier: String = Identifier
    val ChangeType: String = "\\bINSERT|\\bDELETE|\\bUPDATE"
    s"(?s)table ($SchemaIdentifier)\\.($TableIdentifier): ($ChangeType): (.+)".r
  }

  val KeyValuePair: Regex = s"($FieldIdentifier)\\[($TypeDeclaration)\\]:($Value)".r

  def parseKeyValuePairs(keyValuePairs: String): List[Field] =
    KeyValuePair
      .findAllMatchIn(keyValuePairs)
      .collect {
        case regexMatch if regexMatch.groupCount == 3 =>
          // note that there is group 0 that denotes the entire match - and it is not included in the groupCount
          val columnName: String = regexMatch.group(1)
          val columnType: String = regexMatch.group(2)
          val value: String = regexMatch.group(3) match {
            case SingleQuotedString2(content) => content
            case other => other
          }
          Field(columnName, columnType, value)
      }
      .toList

  def transformSlotChanges(slotChanges: List[SlotChange],
                           ignoreTables: List[String],
                           ignoreColumns: Map[String, List[String]]): List[ChangeSet] = {

    def parseKeyValuePairsWithFilter(tableName: String, keyValuePair: String): List[Field] =
      parseKeyValuePairs(keyValuePair).filterNot(f => ignoreColumns.get(tableName).exists(_.contains(f.columnName)))

    slotChanges.groupBy(_.transactionId).map {

      case (transactionId: Long, slotChanges: List[SlotChange]) =>
        val changes: List[Change] = slotChanges.collect {

          case SlotChange(_, ChangeStatement(schemaName, tableName, "UPDATE", changesStr))
              if !ignoreTables.contains(tableName) =>
            RowUpdated(schemaName, tableName, parseKeyValuePairsWithFilter(tableName, changesStr))

          case SlotChange(_, ChangeStatement(schemaName, tableName, "DELETE", changesStr))
              if !ignoreTables.contains(tableName) =>
            RowDeleted(schemaName, tableName, parseKeyValuePairsWithFilter(tableName, changesStr))

          case SlotChange(_, ChangeStatement(schemaName, tableName, "INSERT", changesStr))
              if !ignoreTables.contains(tableName) =>
            RowInserted(schemaName, tableName, parseKeyValuePairsWithFilter(tableName, changesStr))
        }

        ChangeSet(transactionId, changes)

    }
  }.filter(_.changes.nonEmpty).toList.sortBy(_.transactionId)

}
