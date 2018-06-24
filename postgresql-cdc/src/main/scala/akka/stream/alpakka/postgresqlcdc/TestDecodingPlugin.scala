/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import akka.stream.alpakka.postgresqlcdc.scaladsl.Field

import scala.util.matching.Regex

private[postgresqlcdc] object TestDecodingPlugin {

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

  val SchemaIdentifier: String = Identifier

  val TableIdentifier: String = Identifier

  val FieldIdentifier: String = Identifier

  val ChangeType: String = "\\bINSERT|\\bDELETE|\\bUPDATE"

  val TypeDeclaration: String = "[a-zA-Z0-9 ]+"
  // matches: character varying, integer, xml etc.

  val NonStringValue: String = "[^ \"']+"
  // matches: true, false, 3.14, 42 etc.

  val Value: String = s"(?:$NonStringValue)|(?:$SingleQuotedString1)"
  // matches: true, false, 3.14 or 'Strings can have spaces'

  val ChangeStatement: Regex =
    s"(?s)table ($SchemaIdentifier)\\.($TableIdentifier): ($ChangeType): (.+)".r

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

}
