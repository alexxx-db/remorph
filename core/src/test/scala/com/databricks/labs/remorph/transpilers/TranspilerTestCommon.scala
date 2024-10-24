package com.databricks.labs.remorph.transpilers

import com.databricks.labs.remorph.{KoResult, OkResult, PartialResult}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import upickle.default._

trait TranspilerTestCommon extends Matchers with Formatter {

  protected def transpiler: Transpiler

  implicit class TranspilerTestOps(input: String) {
    def transpilesTo(expectedOutput: String): Assertion = {
      transpiler.transpile(SourceCode(input)) match {
        case OkResult(output) => format(output) shouldBe format(expectedOutput)
        case PartialResult(_, err) => fail(write(err))
        case KoResult(_, err) => fail(write(err))
      }
    }
    def failsTranspilation: Assertion = {
      transpiler.transpile(SourceCode(input)) match {
        case KoResult(_, _) => succeed
        case PartialResult(_, _) => succeed
        case _ => fail("query was expected to fail transpilation but didn't")
      }
    }
  }
}
