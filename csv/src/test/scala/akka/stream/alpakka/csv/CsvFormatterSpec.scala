/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv

import java.nio.charset.StandardCharsets

import akka.stream.alpakka.csv.impl.CsvFormatter
import akka.stream.alpakka.csv.scaladsl.CsvQuotingStyle
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CsvFormatterSpec extends AnyWordSpec with Matchers with LogCapturing {

  "CSV Formatter comma as delimiter" should {
    val formatter = new CsvFormatter(',', '\"', '\\', "\r\n", CsvQuotingStyle.Required)

    "format Strings" in {
      expectInOut(formatter, "ett", "två", "tre")("ett,två,tre\r\n")
    }

    "format Strings containing commas" in {
      expectInOut(formatter, "ett", "t,vå", "tre")("ett,\"t,vå\",tre\r\n")
    }

    "format Strings containing quotes" in {
      expectInOut(formatter, "ett", "t\"vå", "tre")("ett,\"t\"\"vå\",tre\r\n")
    }

  }

  "CSV Formatter quoting everything" should {
    val formatter = new CsvFormatter(',', '\"', '\\', "\r\n", CsvQuotingStyle.Always)

    "format Strings" in {
      expectInOut(formatter, "ett", "två", "tre")(""""ett","två","tre"""" + "\r\n")
    }

    "format Strings with commas" in {
      expectInOut(formatter, "ett", "t,vå", "tre")(""""ett","t,vå","tre"""" + "\r\n")
    }

    "format Strings containing quotes" in {
      expectInOut(formatter, "ett", "t\"vå", "tre")(""""ett","t""vå","tre"""" + "\r\n")
    }

    "format Strings containing quotes twice" in {
      expectInOut(formatter, "ett", "t\"v\"å", "tre")(""""ett","t""v""å","tre"""" + "\r\n")
    }

  }

  "CSV Formatter with required quoting" should {
    val formatter = new CsvFormatter(';', '\"', '\\', "\r\n", CsvQuotingStyle.Required)

    "format Strings" in {
      expectInOut(formatter, "ett", "två", "tre")("ett;två;tre\r\n")
    }

    "quote Strings with delimiters" in {
      expectInOut(formatter, "ett", "t;vå", "tre")("ett;\"t;vå\";tre\r\n")
    }

    "quote Strings with quotes" in {
      expectInOut(formatter, "ett", "t\"vå", "tre")("""ett;"t""vå";tre""" + "\r\n")
    }

    "quote Strings with quote at end" in {
      expectInOut(formatter, "ett", "två\"", "tre")("ett;\"två\"\"\";tre\r\n")
    }

    "quote Strings with just a quote" in {
      expectInOut(formatter, "ett", "\"", "tre")("ett;\"\"\"\";tre\r\n")
    }

    "quote Strings containing LF" in {
      expectInOut(formatter, "ett", "\n", "tre")("ett;\"\n\";tre\r\n")
    }

    "quote Strings containing CR, LF" in {
      expectInOut(formatter, "ett", "prefix\r\npostfix", "tre")("ett;\"prefix\r\npostfix\";tre\r\n")
    }

    "duplicate escape char" in {
      expectInOut(formatter, "ett", "prefix\\postfix", "tre")("ett;\"prefix\\\\postfix\";tre\r\n")
    }

    "duplicate escape chars and quotes" in {
      expectInOut(formatter, "ett", "one\\two\"three\\four", "tre")("ett;\"one\\\\two\"\"three\\\\four\";tre\r\n")
    }
  }

  "CSV Formatter with non-standard charset" should {
    val charset = StandardCharsets.UTF_16LE
    val formatter = new CsvFormatter(';', '\"', '\\', "\r\n", CsvQuotingStyle.Required, charset)

    "get the encoding right" in {
      val csv = formatter.toCsv(List("ett", "två", "อักษรไทย"))
      val arr1 = new Array[Byte](csv.length)
      csv.copyToArray(arr1)
      new String(arr1, charset) should be("ett;två;อักษรไทย\r\n")
    }
  }

  private def expectInOut(formatter: CsvFormatter, in: String*)(expect: String): Unit =
    formatter.toCsv(in.toList).utf8String should be(expect)

}
