/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import org.scalatest.{FunSuite, Matchers}
import akka.stream.alpakka.postgresqlcdc.PgSQLChangeDataCaptureSourceStage.Grammar._

class TestParsing extends FunSuite with Matchers {

  test("COMMIT and BEGIN regular expressions") {

    "BEGIN 2379" should fullyMatch regex (Begin withGroups "2379")

    "COMMIT 2213 (at 2018-04-09 05:52:42.626311+00)" should fullyMatch regex (Commit
    withGroups ("2213", "2018-04-09", "05:52:42.626311+00"))

    "COMMIT 2380 (at 2018-04-09 17:56:36.730413+00)" should fullyMatch regex (Commit
    withGroups ("2380", "2018-04-09", "17:56:36.730413+00"))
  }

  test("regular expression for UPDATE, INSERT, DELETE log statements") {

    val ex1 = "table public.\"Users\": UPDATE: ..."

    ex1 should fullyMatch regex (ChangeStatement withGroups ("public", "\"Users\"", "UPDATE", "..."))

    val ex2 = "table public.\"Users\": INSERT: ..."

    ex2 should fullyMatch regex (ChangeStatement withGroups ("public", "\"Users\"", "INSERT", "..."))

    val ex3 = "table public.\"Users\": DELETE: ..."

    ex3 should fullyMatch regex (ChangeStatement withGroups ("public", "\"Users\"", "DELETE", "..."))

  }

  test("regular expression for double quoted string") {

    val ex1 = """"Hello \"world\"""""
    ex1 should fullyMatch regex (s"($DoubleQuotedString)".r withGroups """"Hello \"world\""""")

  }

  test("regular expression for single quoted strings") {

    val ex1 = "'Hello world'"
    ex1 should fullyMatch regex (s"($SingleQuotedString1)".r withGroups "'Hello world'")
    ex1 should fullyMatch regex (SingleQuotedString2 withGroups "Hello world")

    val ex2 = "'Hello ''world'''"
    ex2 should fullyMatch regex (s"($SingleQuotedString1)".r withGroups "'Hello ''world'''")
    ex2 should fullyMatch regex (SingleQuotedString2 withGroups "Hello ''world''")

  }

  test("regular expression for identifiers") {

    val ex1 = "users"
    ex1 should fullyMatch regex Identifier.r

    val ex2 = """"users""""
    ex2 should fullyMatch regex Identifier.r

    val ex3 = "'users'"
    ex3 shouldNot fullyMatch regex Identifier.r

  }

  test("regular expression for type declaration") {

    val ex1 = "integer"

    ex1 should fullyMatch regex TypeDeclaration.r

    val ex2 = "character varying"
    ex2 should fullyMatch regex TypeDeclaration.r

  }

  test("regular expression for values") {

    val ex1 = "'scala'"
    ex1 should fullyMatch regex Value
    ex1 shouldNot fullyMatch regex NonStringValue

    val ex2 = "true"
    ex2 should fullyMatch regex Value
    ex2 should fullyMatch regex NonStringValue
    ex2 shouldNot fullyMatch regex SingleQuotedString1

    val ex3 = "3.14"
    ex3 should fullyMatch regex Value
    ex3 should fullyMatch regex NonStringValue
    ex3 shouldNot fullyMatch regex SingleQuotedString1

  }

  test("regular expression for key value pairs") {

    val ex1 = """"Id"[integer]:1"""

    ex1 should fullyMatch regex KeyValuePair.withGroups("\"Id\"", "integer", "1")

    val ex2 = """"Name"[character varying]:'scala'"""
    ex2 should fullyMatch regex KeyValuePair.withGroups("\"Name\"", "character varying", "'scala'")

  }
}
