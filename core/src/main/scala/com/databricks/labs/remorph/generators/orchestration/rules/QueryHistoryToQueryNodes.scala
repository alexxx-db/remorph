package com.databricks.labs.remorph.generators.orchestration.rules

import com.databricks.labs.remorph.{KoResult, OkResult, Parsing, PartialResult, Transformation, TransformationConstructors, TranspilerState}
import com.databricks.labs.remorph.discovery.{ExecutedQuery, QueryHistory}
import com.databricks.labs.remorph.generators.orchestration.rules.history.{FailedQuery, Migration, PartialQuery, QueryPlan, RawMigration}
import com.databricks.labs.remorph.intermediate.Rule
import com.databricks.labs.remorph.intermediate.workflows.JobNode
import com.databricks.labs.remorph.parsers.PlanParser

class QueryHistoryToQueryNodes(val parser: PlanParser[_]) extends Rule[JobNode] with TransformationConstructors {
  override def apply(plan: JobNode): Transformation[JobNode] = ok(plan match {
    case RawMigration(QueryHistory(queries)) => Migration(queries.par.map(executedQuery).seq)
  })

  private def executedQuery(query: ExecutedQuery): JobNode = {
    val state = TranspilerState(Parsing(query.source, query.id))
    parser.parse
      .flatMap(parser.visit)
      .flatMap(parser.optimize)
      .run(state) match {
      case OkResult((_, plan)) => QueryPlan(plan, query)
      case PartialResult((_, plan), error) => PartialQuery(query, error.msg, QueryPlan(plan, query))
      case KoResult(stage, error) => FailedQuery(query, error.msg, stage)
    }
  }
}
