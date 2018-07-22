/*
 * Copyright (C) 2016-2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.stream.alpakka.postgresqlcdc

import java.time.{LocalDate, LocalTime, ZoneId}

import akka.stream.alpakka.postgresqlcdc.TestDecodingPlugin._
import fastparse.core.Parsed
import fastparse.core.Parsed.Success
import org.scalatest.{FunSuite, Matchers}

import scala.collection.Map

class TestParsing extends FunSuite with Matchers {

  test("parsing double quoted string") {

    val ex1 = "\"Hello\""
    doubleQuotedString.parse(ex1) should matchPattern { case Parsed.Success("\"Hello\"", _) ⇒ }

    val ex2 = "\"Hello \\\"world\\\"\""
    doubleQuotedString.parse(ex2) should matchPattern { case Parsed.Success("\"Hello \\\"world\\\"\"", _) ⇒ }

    val ex3 = """"users""""
    doubleQuotedString.parse(ex3) should matchPattern { case Parsed.Success("\"users\"", _) ⇒ }

  }

  test("parsing single quoted strings") {

    val ex1 = "'Hello world'"
    singleQuotedString.parse(ex1) should matchPattern { case Parsed.Success("Hello world", _) ⇒ }

    val ex2 = "'Hello ''world'''"
    singleQuotedString.parse(ex2) should matchPattern { case Parsed.Success("Hello ''world''", _) ⇒ }

  }

  test("parsing unquoted strings") {

    val ex1 = "some_thing_42"

    unquotedIdentifier.parse(ex1) should matchPattern { case Parsed.Success("some_thing_42", _) ⇒ }

    val ex2 = "\""

    unquotedIdentifier.parse(ex2) should not matchPattern { case Parsed.Success(_, _) ⇒ }

  }

  test("parsing identifiers") {

    val ex1 = "users"
    identifier.parse(ex1) should matchPattern { case Parsed.Success("users", _) ⇒ }

    val ex2 = "\"USERS\""
    identifier.parse(ex2) should matchPattern { case Parsed.Success("\"USERS\"", _) ⇒ }

    val ex3 = "'users'"
    identifier.parse(ex3) should not matchPattern { case Parsed.Success(_, _) ⇒ }

  }

  test("parsing type declarations") {

    val ex1 = "[integer]"
    typeDeclaration.parse(ex1) should matchPattern { case Parsed.Success("integer", _) ⇒ }

    val ex2 = "[character varying]"
    typeDeclaration.parse(ex2) should matchPattern { case Parsed.Success("character varying", _) ⇒ }

  }

  test("parsing values") {

    val ex1 = "'scala'"
    TestDecodingPlugin.value.parse(ex1) should matchPattern { case Parsed.Success("scala", _) ⇒ }

    val ex2 = "true"
    TestDecodingPlugin.value.parse(ex2) should matchPattern { case Parsed.Success("true", _) ⇒ }

    val ex3 = "3.14"
    TestDecodingPlugin.value.parse(ex3) should matchPattern { case Parsed.Success("3.14", _) ⇒ }

    val ex4 = """'<foo><bar id="42"></bar></foo>'"""
    TestDecodingPlugin.value.parse(ex4) should matchPattern {
      case Parsed.Success("""<foo><bar id="42"></bar></foo>""", _) ⇒
    }

    val ex5 = "'<foo>\n<bar id=\"42\">\n</bar>\n</foo>'"
    TestDecodingPlugin.value.parse(ex5) should matchPattern {
      case Parsed.Success("<foo>\n<bar id=\"42\">\n</bar>\n</foo>", _) ⇒
    }

  }

  test("parsing fields") {

    val ex1 = "a[integer]:1"
    data.parse(ex1) should matchPattern {
      case Parsed.Success(List(Field("a", "integer", "1")), _) ⇒
    }

    val ex2 = "a[integer]:1 b[integer]:2"
    data.parse(ex2) should matchPattern {
      case Parsed.Success(List(Field("a", "integer", "1"), Field("b", "integer", "2")), _) ⇒
    }

  }

  test("parsing BEGIN and COMMIT statements") {

    begin.parse("BEGIN 2379") should matchPattern { case Parsed.Success(BeginStatement(2379), _) ⇒ }

    date.parse("2018-04-09") should matchPattern { case Parsed.Success(localDate: LocalDate, _) ⇒ }

    time.parse("05:52:42.626311+00") should matchPattern {
      case Parsed.Success((localTime: LocalTime, zoneId: ZoneId), _) ⇒
    }

    commit.parse("COMMIT 2213 (at 2018-04-09 05:52:42.626311+00)") should matchPattern {
      case Parsed.Success(CommitStatement(2213, zonedDateTime), _) ⇒
    }

  }

  test("parsing UPDATE, INSERT, DELETE log statements") {

    import fastparse.all._

    val changeStatementTest = P(changeStatement).map(changeBuilder ⇒ changeBuilder(("unknown", 0)))

    val ex1 = "table public.abc: UPDATE: a[integer]:1 b[integer]:1 c[integer]:3"

    val ex1ExpectedDataNew = Map("a" → "1", "b" → "1", "c" → "3")
    val ex1ExpectedDataOld = Map.empty[String, String]
    val ex1ExpectedSchemaNew = Map("a" → "integer", "b" → "integer", "c" → "integer")
    val ex1ExpectedSchemaOld = Map.empty[String, String]

    changeStatementTest.parse(ex1) should matchPattern {
      case Success(r @ RowUpdated("public", "abc", "unknown", 0, `ex1ExpectedDataNew`, `ex1ExpectedDataOld`), _)
          if r.schemaOld == ex1ExpectedSchemaOld && r.schemaNew == ex1ExpectedSchemaNew => // success
    }

    val ex2 = "table public.sales: UPDATE: id[integer]:0 info[jsonb]:'{\"name\": \"alpakka\", \"countries\": [\"*\"]}'"

    val ex2ExpectedDataNew = Map("id" → "0", "info" → "{\"name\": \"alpakka\", \"countries\": [\"*\"]}")
    val ex2ExpectedDataOld = Map.empty[String, String]
    val ex2ExpectedSchemaNew = Map("id" → "integer", "info" → "jsonb")
    val ex2ExpectedSchemaOld = Map.empty[String, String]

    changeStatementTest.parse(ex2) should matchPattern {
      case Success(r @ RowUpdated("public", "sales", "unknown", 0, `ex2ExpectedDataNew`, `ex2ExpectedDataOld`), _)
          if r.schemaOld == ex2ExpectedSchemaOld && r.schemaNew == ex2ExpectedSchemaNew ⇒ // success
    }

    val ex3 =
      "table public.abc: UPDATE: old-key: a[integer]:3 b[integer]:2 new-tuple: a[integer]:1 b[integer]:2 c[integer]:3"

    val ex3ExpectedDataNew = Map("a" → "1", "b" → "2", "c" → "3")
    val ex3ExpectedDataOld = Map("a" → "3", "b" → "2")
    val ex3ExpectedSchemaNew = Map("a" → "integer", "b" → "integer", "c" → "integer")
    val ex3ExpectedSchemaOld = Map("a" → "integer", "b" → "integer")

    changeStatementTest.parse(ex3) should matchPattern {
      case Success(r @ RowUpdated("public", "abc", "unknown", 0, `ex3ExpectedDataNew`, `ex3ExpectedDataOld`), _)
          if r.schemaOld == ex3ExpectedSchemaOld && r.schemaNew == ex3ExpectedSchemaNew ⇒ // success
    }

  }

}
