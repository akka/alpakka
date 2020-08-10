/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.csv

import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}

import akka.stream.alpakka.csv.impl.CsvParser
import akka.stream.alpakka.csv.scaladsl.ByteOrderMark
import akka.stream.alpakka.testkit.scaladsl.LogCapturing
import akka.util.ByteString
import org.scalatest.OptionValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CsvParserSpec extends AnyWordSpec with Matchers with OptionValues with LogCapturing {

  val maximumLineLength = 10 * 1024

  "CSV parser" should {
    "read comma separated values into a list" in {
      expectInOut("one,two,three\n", List("one", "two", "three"))
    }

    "not deliver input until end of line is reached" in {
      expectInOut("one,two,three")
    }

    "read a line if line end is not required" in {
      expectInOut("one,two,three", List("one", "two", "three"))(requireLineEnd = false)
    }

    "read two lines into two lists" in {
      expectInOut("one,two,three\n1,2,3\n", List("one", "two", "three"), List("1", "2", "3"))
    }

    "parse empty input to None" in {
      val in = ByteString.empty
      val parser = new CsvParser(',', '-', '.', maximumLineLength)
      parser.offer(in)
      parser.poll(requireLineEnd = true) should be('empty)
    }

    "parse leading comma to be an empty column" in {
      expectInOut(",one,two,three\n", List("", "one", "two", "three"))
    }

    "parse trailing comma to be an empty column" in {
      expectInOut("one,two,three,\n", List("one", "two", "three", ""))
    }

    "parse double comma to be an empty column" in {
      expectInOut("one,,two,three\n", List("one", "", "two", "three"))
    }

    "parse an empty line with LF into a single column" in {
      expectInOut("\n", List(""))
    }

    "parse an empty line with CR, LF into a single column" in {
      expectInOut("\r\n", List(""))
    }

    "parse an empty line with CR, CR, LF into a single column" in {
      // https://github.com/akka/alpakka/issues/987
      expectInOut("\r\r\n", List(""))
    }

    "parse UTF-8 chars unchanged" in {
      expectInOut("ℵ,a,ñÅë,อักษรไทย\n", List("ℵ", "a", "ñÅë", "อักษรไทย"))
    }

    "parse double quote chars into empty column" in {
      expectInOut("a,\"\",c\n", List("a", "", "c"))
    }

    "accept mid-column quotes" in {
      expectInOut("11,22\"z\",13\n", List("11", "22\"z\"", "13"))
    }

    "accept quote ending mid-column" in {
      expectInOut("11,\"z\"22,13\n", List("11", "z22", "13"))
    }

    "accept quote ending mid-column (with new line)" in {
      expectInOut("11,\"z\n\"22,13\n", List("11", "z\n22", "13"))
    }

    "parse double quote chars within quotes into one quote" in {
      expectInOut("a,\"\"\"\",c\n", List("a", "\"", "c"))
    }

    "parse double quote chars within quotes inte one quoute at end of value" in {
      expectInOut("\"Venture \"\"Extended Edition\"\"\",\"\",4900.00\n",
                  List("Venture \"Extended Edition\"", "", "4900.00"))
    }

    "parse double escape chars into one escape char" in {
      expectInOut("a,\\\\,c\n", List("a", "\\", "c"))
      expectInOut("a,b\\\\c,d\n", List("a", "b\\c", "d"))
      expectInOut("a\\\\b,c,d\n", List("a\\b", "c", "d"))
      expectInOut("\\\\,a,b\n", List("\\", "a", "b"))
    }

    "parse quoted escape chars into one escape char" in {
      expectInOut("a,\"\\\\\",c\n", List("a", "\\", "c"))
    }

    "parse escaped comma into comma" in {
      expectInOut("a,\\,,c\n", List("a", ",", "c"))
    }

    "parse escape char into itself if not followed by escape or delimiter" in {
      expectInOut("a,\\b,\"\\-c\"\n", List("a", "\\b", "\\-c"))
    }

    "fail on escaped quote as quotes are escaped by doubled quote chars" in {
      val in = ByteString("a,\\\",c\n")
      val parser = new CsvParser(',', '"', '\\', maximumLineLength)
      parser.offer(in)
      val exception = the[MalformedCsvException] thrownBy {
        parser.poll(requireLineEnd = true)
      }
      exception.getMessage should be("wrong escaping at 1:3, quote is escaped as \"\"")
      exception.getLineNo should be(1)
      exception.getBytePos should be(3)
    }

    "fail on escape at line end" in {
      val in = ByteString("""a,\""")
      val parser = new CsvParser(',', '"', '\\', maximumLineLength)
      parser.offer(in)
      val exception = the[MalformedCsvException] thrownBy {
        parser.poll(requireLineEnd = false)
      }
      exception.getMessage should be("wrong escaping at 1:3, no character after escape")
    }

    "fail on escape within field at line end" in {
      val in = ByteString("""a,b\""")
      val parser = new CsvParser(',', '"', '\\', maximumLineLength)
      parser.offer(in)
      val exception = the[MalformedCsvException] thrownBy {
        parser.poll(requireLineEnd = false)
      }
      exception.getMessage should be("wrong escaping at 1:4, no character after escape")
    }

    "fail on escape within quoted field at line end" in {
      val in = ByteString("""a,"\""")
      val parser = new CsvParser(',', '"', '\\', maximumLineLength)
      parser.offer(in)
      val exception = the[MalformedCsvException] thrownBy {
        parser.poll(requireLineEnd = false)
      }
      exception.getMessage should be("wrong escaping at 1:4, no character after escape")
    }

    "parse escaped escape within quotes into quote" in {
      expectInOut("a,\"\\\\\",c\n", List("a", "\\", "c"))
    }

    "parse escaped quote within quotes into quote" in {
      expectInOut("a,\"\\\"\",c\n", List("a", "\"", "c"))
    }

    "parse escaped escape in text within quotes into quote" in {
      expectInOut("a,\"abc\\\\def\",c\n", List("a", "abc\\def", "c"))
    }

    "parse escaped quote in text within quotes into quote" in {
      expectInOut("a,\"abc\\\"def\",c\n", List("a", "abc\"def", "c"))
    }

    "allow Unicode L SEP 0x2028 as line separator" ignore {
      val in = ByteString("abc\u2028")
      val parser = new CsvParser(',', '"', '\\', maximumLineLength)
      parser.offer(in)
      val res = parser.poll(requireLineEnd = true)
      res.value.map(_.utf8String) should be(List("abc"))
    }

    "ignore trailing \\n" in {
      expectInOut("one,two,three\n", List("one", "two", "three"))
    }

    "ignore trailing \\r\\n" in {
      expectInOut("one,two,three\r\n", List("one", "two", "three"))
    }

    "keep whitespace" in {
      expectInOut("one, two ,three  \n", List("one", " two ", "three  "))
    }

    "quoted values keep whitespace" in {
      expectInOut("\" one \",\" two \",\"three  \"\n", List(" one ", " two ", "three  "))
    }

    "quoted values may contain LF" in {
      expectInOut("""one,"two
          |two",three
          |1,2,3
          |""".stripMargin, List("one", "two\ntwo", "three"), List("1", "2", "3"))
    }

    "quoted values may contain CR, LF" in {
      expectInOut("one,\"two\r\ntwo\",three\n", List("one", "two\r\ntwo", "three"))
      expectInOut("one,\"two\r\ntwo\",three", List("one", "two\r\ntwo", "three"))(requireLineEnd = false)
    }

    "quoted values may contain CR, LF in last field" in {
      expectInOut("one,\"two\r\ntwo\"", List("one", "two\r\ntwo"))(requireLineEnd = false)
    }

    "handle escaping split over two inputs" in {
      splitInput("\\", "\\A,B", List("\\A", "B"))
      splitInput("A\\", "\\A,B", List("A\\A", "B"))
      splitInput("A,\\", "\\B", List("A", "\\B"))
      splitInput("A,B\\", "\\", List("A", "B\\"))
    }

    "handle escaping withing quotes, split over two inputs" in {
      splitInput("\"\\", "\\A\",B", List("\\A", "B"))
      splitInput("\"A\\", "\\A\",B", List("A\\A", "B"))
      splitInput("A,\"\\", "\\B\"", List("A", "\\B"))
      splitInput("A,\"B\\", "\\\"", List("A", "B\\"))
    }

    "handle double quotes split over two inputs" in {
      splitInput("\"\"", "\"A\",B", List("\"A", "B"))
      splitInput("\"A\"", "\"\",B", List("A\"", "B"))
      splitInput("\"A\"", "\"A\",B", List("A\"A", "B"))
      splitInput("A,\"\"", "\"B\"", List("A", "\"B"))
      splitInput("A,\"B\"", "\"B\"", List("A", "B\"B"))
      splitInput("A,\"B\"", "\"\"", List("A", "B\""))
    }

    def splitInput(in1: String, in2: String, expect: List[String]) = {
      val parser = new CsvParser(delimiter = ',', quoteChar = '"', escapeChar = '\\', maximumLineLength)
      parser.offer(ByteString(in1))
      parser.poll(requireLineEnd = true) should be('empty)
      parser.offer(ByteString(in2) ++ ByteString("\n"))
      parser.poll(requireLineEnd = true).value.map(_.utf8String) should be(expect)
      parser.poll(requireLineEnd = true) should be('empty)
    }

    "fail for unclosed quotes at end of input" in {
      val parser = new CsvParser(delimiter = ',', quoteChar = '"', escapeChar = '\\', maximumLineLength)
      parser.offer(ByteString("\"A\""))
      parser.poll(requireLineEnd = true) should be('empty)
      parser.offer(ByteString("\",B"))
      parser.poll(requireLineEnd = true) should be('empty)
      val exception = the[MalformedCsvException] thrownBy {
        parser.poll(requireLineEnd = false)
      }
      exception.getMessage should be("unclosed quote at end of input 1:6, no matching quote found")
    }

    "accept delimiter as last input" in {
      val parser = new CsvParser(delimiter = ',', quoteChar = '"', escapeChar = '\\', maximumLineLength)
      parser.offer(ByteString("A,B\nA,"))
      parser.poll(requireLineEnd = false).value.map(_.utf8String) should be(List("A", "B"))
      parser.poll(requireLineEnd = false).value shouldBe List(ByteString("A"), ByteString.empty)
    }

    "accept delimiter as last input on first line" in {
      val parser = new CsvParser(delimiter = ',', quoteChar = '"', escapeChar = '\\', maximumLineLength)
      parser.offer(ByteString("A,"))
      parser.poll(requireLineEnd = false).value shouldBe List(ByteString("A"), ByteString.empty)
    }

    "detect line ending correctly if input is split between CR & LF" in {
      val parser = new CsvParser(delimiter = ',', quoteChar = '"', escapeChar = '\\', maximumLineLength)
      parser.offer(ByteString("A,D\r"))
      parser.poll(requireLineEnd = true) should be('empty)
      parser.offer(ByteString("\nB,E\r\n"))
      parser.poll(requireLineEnd = true).value.map(_.utf8String) should be(List("A", "D"))
      parser.poll(requireLineEnd = true).value.map(_.utf8String) should be(List("B", "E"))
      parser.poll(requireLineEnd = true) should be('empty)
    }

    "detect line ending correctly if input is split between CR, CR & LF" in {
      val parser = new CsvParser(delimiter = ',', quoteChar = '"', escapeChar = '\\', maximumLineLength)
      parser.offer(ByteString("A,D\r"))
      parser.poll(requireLineEnd = true) should be('empty)
      parser.offer(ByteString("\r"))
      parser.poll(requireLineEnd = true) should be('empty)
      parser.offer(ByteString("\nB,E\r\n"))
      parser.poll(requireLineEnd = true).value.map(_.utf8String) should be(List("A", "D"))
      parser.poll(requireLineEnd = true).value.map(_.utf8String) should be(List("B", "E"))
      parser.poll(requireLineEnd = true) should be('empty)
    }

    "take double \" as single" in {
      expectInOut("one,\"tw\"\"o\",three\n", List("one", "tw\"o", "three"))
    }

    "read values with different separator" in {
      expectInOut("$Foo $#$Bar $#$Baz $\n", List("Foo ", "Bar ", "Baz "))(delimiter = '#',
                                                                          quoteChar = '$',
                                                                          escapeChar = '\\')
    }

    "fail on a very 'long' line" in {
      val in = ByteString("a,b,c\n1,3,5,7,9,1\n")
      val parser = new CsvParser(',', '"', '\\', 11)
      parser.offer(in)
      parser.poll(requireLineEnd = true)
      val exception = the[MalformedCsvException] thrownBy {
        parser.poll(requireLineEnd = true)
      }
      exception.getMessage should be("no line end encountered within 11 bytes on line 2")
    }

  }

  "CSV parsing with Byte Order Mark" should {
    "accept UTF-8 BOM" in {
      val in = ByteOrderMark.UTF_8 ++ ByteString("one,two,three\n", StandardCharsets.UTF_8.name())
      expectBsInOut(in, List("one", "two", "three"))
    }

    "handle quote right after UTF-8 BOM" in {
      val in = ByteOrderMark.UTF_8 ++ ByteString("\"one\",\"two\",\"three\"\n", StandardCharsets.UTF_8.name())
      expectBsInOut(in, List("one", "two", "three"))
    }

    "fail for UTF-16 LE BOM" in {
      val in = ByteOrderMark.UTF_16_LE ++ ByteString("one,two,three\n", StandardCharsets.UTF_16LE.name())
      a[UnsupportedCharsetException] should be thrownBy expectBsInOut(in, List("one", "two", "three"))
    }

    "fail for UTF-16 BE BOM" in {
      val in = ByteOrderMark.UTF_16_BE ++ ByteString("one,two,three\n", StandardCharsets.UTF_16BE.name())
      a[UnsupportedCharsetException] should be thrownBy expectBsInOut(in, List("one", "two", "three"))
    }

    "fail for UTF-32 LE BOM" in {
      val in = ByteOrderMark.UTF_32_LE ++ ByteString("one,two,three\n", "UTF-32LE")
      a[UnsupportedCharsetException] should be thrownBy expectBsInOut(in, List("one", "two", "three"))
    }

    "fail for UTF-32 BE BOM" in {
      val in = ByteOrderMark.UTF_32_BE ++ ByteString("one,two,three\n", "UTF-32BE")
      a[UnsupportedCharsetException] should be thrownBy expectBsInOut(in, List("one", "two", "three"))
    }
  }

  def expectInOut(in: String, expected: List[String]*)(implicit delimiter: Byte = ',',
                                                       quoteChar: Byte = '"',
                                                       escapeChar: Byte = '\\',
                                                       requireLineEnd: Boolean = true): Unit = {
    val bsIn = ByteString(in)
    expectBsInOut(bsIn, expected: _*)(delimiter, quoteChar, escapeChar, requireLineEnd)
  }

  def expectBsInOut(bsIn: ByteString, expected: List[String]*)(implicit delimiter: Byte = ',',
                                                               quoteChar: Byte = '"',
                                                               escapeChar: Byte = '\\',
                                                               requireLineEnd: Boolean = true): Unit = {
    val parser = new CsvParser(delimiter, quoteChar, escapeChar, maximumLineLength)
    parser.offer(bsIn)
    expected.foreach { out =>
      parser.poll(requireLineEnd).value.map(_.utf8String) should be(out)
    }
    parser.poll(requireLineEnd = true) should be('empty)
  }

}
