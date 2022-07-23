/*
 * Copyright (C) since 2016 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.stream.alpakka.typesense.scaladsl

import akka.stream.alpakka.typesense.FilterDeleteDocumentsQuery
import org.scalatest.Assertion
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should

import java.util

class FilterDeleteDocumentQueryDslSpec extends AnyFunSpec with should.Matchers {
  describe("Should prepare proper query") {
    def check(query: FilterDeleteDocumentsQuery, expected: String): Assertion = query.asTextQuery shouldBe expected

    it("test query") {
      check(query = FilterDeleteDocumentsQueryDsl.stringQuery("budget:>150"), expected = "budget:>150")
    }

    it("in string set") {
      check(query = FilterDeleteDocumentsQueryDsl.inStringSet("id", Seq("id1", "id2", "id3")),
            expected = "id:[id1,id2,id3]")
    }

    it("in int set") {
      check(query = FilterDeleteDocumentsQueryDsl.inIntSet("budget", Seq(10, 20, 30)), expected = "budget:[10,20,30]")
    }

    it("bigger than int") {
      check(query = FilterDeleteDocumentsQueryDsl.biggerThanInt("budget", 2000), expected = "budget:>2000")
    }

    it("bigger than float") {
      check(query = FilterDeleteDocumentsQueryDsl.biggerThanFloat("evaluation", 2.3), expected = "evaluation:>2.3")
    }

    it("bigger than or equal int") {
      check(query = FilterDeleteDocumentsQueryDsl.biggerThanOrEqualInt("budget", 2000), expected = "budget:>=2000")
    }

    it("bigger than or equal float") {
      check(query = FilterDeleteDocumentsQueryDsl.biggerThanOrEqualFloat("evaluation", 2.3),
            expected = "evaluation:>=2.3")
    }

    it("lower than int") {
      check(query = FilterDeleteDocumentsQueryDsl.lowerThanInt("budget", 2000), expected = "budget:<2000")
    }

    it("lower than float") {
      check(query = FilterDeleteDocumentsQueryDsl.lowerThanFloat("evaluation", 2.3), expected = "evaluation:<2.3")
    }

    it("lower than or equal int") {
      check(query = FilterDeleteDocumentsQueryDsl.lowerThanOrEqualInt("budget", 2000), expected = "budget:<=2000")
    }

    it("lower than or equal float") {
      check(query = FilterDeleteDocumentsQueryDsl.lowerThanOrEqualFloat("evaluation", 2.3),
            expected = "evaluation:<=2.3")
    }
  }

  describe("Java API should return the same result as Scala API") {
    def check(
        scala: FilterDeleteDocumentsQueryDsl.type => FilterDeleteDocumentsQuery,
        java: akka.stream.alpakka.typesense.javadsl.FilterDeleteDocumentsQueryDsl.type => FilterDeleteDocumentsQuery
    ): Assertion =
      scala(FilterDeleteDocumentsQueryDsl).asTextQuery shouldBe java(
        akka.stream.alpakka.typesense.javadsl.FilterDeleteDocumentsQueryDsl
      ).asTextQuery

    it("string query") {
      check(
        scala = _.stringQuery("budget:>150"),
        java = _.stringQuery("budget:>150")
      )
    }

    it("in string set") {
      check(
        scala = _.inStringSet("id", Seq("id1", "id2", "id3")),
        java = {
          val ids = new util.ArrayList[String]()
          ids.add("id1")
          ids.add("id2")
          ids.add("id3")
          _.inStringSet("id", ids)
        }
      )
    }

    it("in int set") {
      check(
        scala = _.inIntSet("budget", Seq(10, 20, 30)),
        java = {
          val ids = new util.ArrayList[java.lang.Integer]()
          ids.add(10)
          ids.add(20)
          ids.add(30)
          _.inIntSet("budget", ids)
        }
      )
    }

    it("bigger than int") {
      check(
        scala = _.biggerThanInt("budget", 2000),
        java = _.biggerThanInt("budget", 2000)
      )
    }

    it("bigger than float") {
      check(
        scala = _.biggerThanFloat("evaluation", 2.3),
        java = _.biggerThanFloat("evaluation", 2.3)
      )
    }

    it("bigger than or equal int") {
      check(
        scala = _.biggerThanOrEqualInt("budget", 2000),
        java = _.biggerThanOrEqualInt("budget", 2000)
      )
    }

    it("bigger than or equal float") {
      check(
        scala = _.biggerThanOrEqualFloat("evaluation", 2.3),
        java = _.biggerThanOrEqualFloat("evaluation", 2.3)
      )
    }

    it("lower than int") {
      check(
        scala = _.lowerThanInt("budget", 2000),
        java = _.lowerThanInt("budget", 2000)
      )
    }

    it("lower than float") {
      check(
        scala = _.lowerThanFloat("evaluation", 2.3),
        java = _.lowerThanFloat("evaluation", 2.3)
      )
    }

    it("lower than or equal int") {
      check(
        scala = _.lowerThanOrEqualInt("budget", 2000),
        java = _.lowerThanOrEqualInt("budget", 2000)
      )
    }

    it("lower than or equal float") {
      check(
        scala = _.lowerThanOrEqualFloat("evaluation", 2.3),
        java = _.lowerThanOrEqualFloat("evaluation", 2.3)
      )
    }
  }
}
