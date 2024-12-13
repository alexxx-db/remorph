package com.databricks.labs.remorph.generators.orchestration

import com.databricks.labs.remorph.Transformation
import com.databricks.labs.remorph.generators.Generator
import com.databricks.labs.remorph.generators.orchestration.rules.converted.CreatedFile
import com.databricks.labs.remorph.generators.orchestration.rules._
import com.databricks.labs.remorph.intermediate.Rules
import com.databricks.labs.remorph.intermediate.workflows.JobNode
import com.databricks.labs.remorph.parsers.PlanParser
import com.databricks.labs.remorph.transpilers.{PySparkGenerator, SqlGenerator}

class FileSetGenerator(
    private[this] val parser: PlanParser[_],
    private[this] val sqlGen: SqlGenerator,
    private[this] val pyGen: PySparkGenerator)
    extends Generator[JobNode, FileSet] {
  private[this] val rules = Rules(
    new QueryHistoryToQueryNodes(parser),
    new DefineSchemas(),
    new ExtractVariables(),
    new TryGenerateSQL(sqlGen),
    new TryGeneratePythonNotebook(pyGen),
    new TrySummarizeFailures(),
    new ReformatCode(),
    new DefineJob(),
    new GenerateBundleFile())

  override def generate(tree: JobNode): Transformation[FileSet] = {
    rules(tree).map { plan =>
      val fileSet = new FileSet()

      plan foreachUp {
        case CreatedFile(name, code) =>
          fileSet.withFile(name, code)
        case _ => // noop
      }
      fileSet
    }
  }
}
