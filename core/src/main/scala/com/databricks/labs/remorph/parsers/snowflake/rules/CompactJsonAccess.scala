package com.databricks.labs.remorph.parsers.snowflake.rules

import com.databricks.labs.remorph.{Transformation, TransformationConstructors}
import com.databricks.labs.remorph.intermediate._

class CompactJsonAccess extends Rule[LogicalPlan] with IRHelpers with TransformationConstructors {
  override def apply(plan: LogicalPlan): Transformation[LogicalPlan] = plan transformAllExpressions {
    case expression: Expression =>
      expression transform {
        case JsonAccess(JsonAccess(l1, r1), JsonAccess(l2, r2)) => ok(JsonAccess(l1, Dot(r1, Dot(l2, r2))))
        case JsonAccess(JsonAccess(l1, r1), r2) => ok(JsonAccess(l1, Dot(r1, r2)))
        case JsonAccess(l1, JsonAccess(l2, r2)) => ok(JsonAccess(l1, Dot(l2, r2)))
      }
  }
}
