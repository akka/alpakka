package akka.stream.alpakka.csv

import akka.stream.alpakka.csv.CsvParser.MalformedCSVException
import akka.util.ByteString
import org.scalatest.{Matchers, OptionValues, WordSpec}

class CsvParserSpec extends WordSpec with Matchers with OptionValues {

  "CSV parser" should {
    "read comma separated values into a list" in {
      val in = ByteString("one,two,three")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("one", "two", "three"))
      parser.poll() should be('empty)
    }

    "read two lines into two lists" in {
      val in = ByteString("one,two,three\n1,2,3")
      val parser = new CsvParser()
      parser.offer(in)
      val res1 = parser.poll()
      res1.value.map(_.utf8String) should be(List("one", "two", "three"))
      val res2 = parser.poll()
      res2.value.map(_.utf8String) should be(List("1", "2", "3"))
      parser.poll() should be('empty)
    }

    "read two lines with LF at end into two lists" in {
      val in = ByteString("one,two,three\n1,2,3\n")
      val parser = new CsvParser()
      parser.offer(in)
      val res1 = parser.poll()
      res1.value.map(_.utf8String) should be(List("one", "two", "three"))
      val res2 = parser.poll()
      res2.value.map(_.utf8String) should be(List("1", "2", "3"))
      parser.poll() should be('empty)
    }

    "parse empty input to None" in {
      val in = ByteString.empty
      val parser = new CsvParser()
      parser.offer(in)
      parser.poll() should be ('empty)
    }

    "parse leading comma to be an empty column" in {
      val in = ByteString(",one,two,three")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("", "one", "two", "three"))
    }

    "parse trailing comma to be an empty column" in {
      val in = ByteString("one,two,three,")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("one", "two", "three", ""))
    }

    "parse double comma to be an empty column" in {
      val in = ByteString("one,,two,three")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("one", "", "two", "three"))
    }

    "parse an empty line with LF into a single column" in {
      val in = ByteString("\n")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List(""))
      parser.poll() should be ('empty)
    }

    "parse an empty line with CR, LF into a single column" in {
      val in = ByteString("\r\n")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List(""))
    }

    "parse UTF-8 chars unchanged" in {
      val in = ByteString("""a,ñÅë,อักษรไทย""", ByteString.UTF_8)
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "ñÅë", "อักษรไทย"))
    }

    "parse double quote chars into empty column" in {
      val in = ByteString("""a,"",c""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "", "c"))
    }

    "parse double quote chars within quotes into one quote" in {
      val in = ByteString("a,\"\"\"\",c")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "\"", "c"))
    }

    "parse double escape chars into one escape char" in {
      val in = ByteString("""a,\\,c""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "\\", "c"))
    }

    "parse escaped comma into comma" in {
      val in = ByteString("""a,\,,c""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", ",", "c"))
    }

    "fail on escaped quote as quotes are escaped by doubled quote chars" in {
      val in = ByteString("""a,\",c""")
      val parser = new CsvParser()
      parser.offer(in)
      assertThrows[MalformedCSVException] {
        parser.poll()
      }
    }

    "fail on escape at line end" in {
      val in = ByteString("""a,\""")
      val parser = new CsvParser()
      parser.offer(in)
      assertThrows[MalformedCSVException] {
        parser.poll()
      }
    }

    "fail on escape within field at line end" in {
      val in = ByteString("""a,b\""")
      val parser = new CsvParser()
      parser.offer(in)
      assertThrows[MalformedCSVException] {
        parser.poll()
      }
    }

    "fail on escape within quoted field at line end" in {
      val in = ByteString("""a,"\""")
      val parser = new CsvParser()
      parser.offer(in)
      assertThrows[MalformedCSVException] {
        parser.poll()
      }
    }

    "parse escaped escape within quotes into quote" in {
      val in = ByteString("""a,"\\",c""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "\\", "c"))
    }

    "parse escaped quote within quotes into quote" in {
      val in = ByteString("""a,"\"",c""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "\"", "c"))
    }

    "parse escaped escape in text within quotes into quote" in {
      val in = ByteString("""a,"abc\\def",c""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "abc\\def", "c"))
    }

    "parse escaped quote in text within quotes into quote" in {
      val in = ByteString("""a,"abc\"def",c""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("a", "abc\"def", "c"))
    }

    "empty line special results in single column" ignore {
      val in = ByteString("\u2028")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List(""))
    }

    "ignore trailing \\n" in {
      val in = ByteString("one,two,three\n")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("one", "two", "three"))
      parser.poll() should be ('empty)
    }

    "ignore trailing \\n\\r" in {
      val in = ByteString("one,two,three\n\r")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("one", "two", "three"))
    }

    "keep whitespace" in {
      val in = ByteString("""one, two ,three  """)
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("one", " two ", "three  "))
    }

    "quoted values keep whitespace" in {
      val in = ByteString("""" one "," two ","three  """")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List(" one ", " two ", "three  "))
    }

    "quoted values may contain LF" in {
      val in = ByteString("""one,"two
          |two",three
          |1,2,3""".stripMargin)
      val parser = new CsvParser()
      parser.offer(in)
      val res1 = parser.poll()
      res1.value.map(_.utf8String) should be(List("one", "two\ntwo", "three"))
      val res2 = parser.poll()
      res2.value.map(_.utf8String) should be(List("1", "2", "3"))
    }

    "quoted values may contain CR, LF" in {
      val in = ByteString("one,\"two\r\ntwo\",three")
      val parser = new CsvParser()
      parser.offer(in)
      val res1 = parser.poll()
      res1.value.map(_.utf8String) should be(List("one", "two\r\ntwo", "three"))
    }

    "take double \" as single" in {
      val in = ByteString("""one,"tw""o",three""")
      val parser = new CsvParser()
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("one", "tw\"o", "three"))
    }

    "read values with different separator" in {
      val in = ByteString("""$Foo $#$Bar $#$Baz $""")
      val parser = new CsvParser('\\', '#', '$')
      parser.offer(in)
      val res = parser.poll()
      res.value.map(_.utf8String) should be(List("Foo ", "Bar ", "Baz "))
    }
  }

}
