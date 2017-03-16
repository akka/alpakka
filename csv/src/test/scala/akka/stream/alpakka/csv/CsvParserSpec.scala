/*
 * Copyright (C) 2016 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.csv

import akka.stream.alpakka.csv.CsvParser.MalformedCsvException
import akka.stream.alpakka.csv.scaladsl.CsvFraming
import akka.util.ByteString
import org.scalatest.{Matchers, OptionValues, WordSpec}

class CsvParserSpec extends WordSpec with Matchers with OptionValues {

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
      val parser = new CsvParser(',', '-', '.')
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

    "parse UTF-8 chars unchanged" in {
      expectInOut("a,ñÅë,อักษรไทย\n", List("a", "ñÅë", "อักษรไทย"))
    }

    "parse double quote chars into empty column" in {
      expectInOut("a,\"\",c\n", List("a", "", "c"))
    }

    "parse double quote chars within quotes into one quote" in {
      expectInOut("a,\"\"\"\",c\n", List("a", "\"", "c"))
    }

    "parse double escape chars into one escape char" in {
      expectInOut("a,\\\\,c\n", List("a", "\\", "c"))
    }

    "parse quoted escape chars into one escape char" in {
      expectInOut("a,\"\\\\\",c\n", List("a", "\\", "c"))
    }

    "parse escaped comma into comma" in {
      expectInOut("a,\\,,c\n", List("a", ",", "c"))
    }

    "fail on escaped quote as quotes are escaped by doubled quote chars" in {
      val in = ByteString("a,\\\",c\n")
      val parser = new CsvParser(',', '"', '\\')
      parser.offer(in)
      assertThrows[MalformedCsvException] {
        parser.poll(requireLineEnd = true)
      }
    }

    "fail on escape at line end" in {
      val in = ByteString("""a,\""")
      val parser = new CsvParser(',', '"', '\\')
      parser.offer(in)
      assertThrows[MalformedCsvException] {
        parser.poll(requireLineEnd = true)
      }
    }

    "fail on escape within field at line end" in {
      val in = ByteString("""a,b\""")
      val parser = new CsvParser(',', '"', '\\')
      parser.offer(in)
      assertThrows[MalformedCsvException] {
        parser.poll(requireLineEnd = true)
      }
    }

    "fail on escape within quoted field at line end" in {
      val in = ByteString("""a,"\""")
      val parser = new CsvParser(',', '"', '\\')
      parser.offer(in)
      assertThrows[MalformedCsvException] {
        parser.poll(requireLineEnd = true)
      }
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

    "empty line special results in single column" ignore {
      val in = ByteString("\u2028")
      val parser = new CsvParser(',', '"', '\\')
      parser.offer(in)
      val res = parser.poll(requireLineEnd = true)
      res.value.map(_.utf8String) should be(List(""))
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
    }

    "take double \" as single" in {
      expectInOut("one,\"tw\"\"o\",three\n", List("one", "tw\"o", "three"))
    }

    "read values with different separator" in {
      expectInOut("$Foo $#$Bar $#$Baz $\n", List("Foo ", "Bar ", "Baz "))(delimiter = '#', quoteChar = '$',
        escapeChar = '\\')
    }
  }

  def expectInOut(in: String, expected: List[String]*)(implicit delimiter: Byte = ',',
                                                       quoteChar: Byte = '"',
                                                       escapeChar: Byte = '\\',
                                                       requireLineEnd: Boolean = true): Unit = {
    val parser = new CsvParser(delimiter, quoteChar, escapeChar)
    parser.offer(ByteString(in))
    expected.foreach { out =>
      parser.poll(requireLineEnd).value.map(_.utf8String) should be(out)
    }
    parser.poll(requireLineEnd = true) should be('empty)
  }

}
