package com.databricks.labs.remorph.generators.orchestration.rules

import com.databricks.labs.remorph.{Transformation, TransformationConstructors}
import com.databricks.labs.remorph.generators.orchestration.rules.bundles._
import com.databricks.labs.remorph.generators.orchestration.rules.converted.CreatedFile
import com.databricks.labs.remorph.generators.orchestration.rules.history.Migration
import com.databricks.labs.remorph.intermediate.Rule
import com.databricks.labs.remorph.intermediate.workflows.JobNode
import com.databricks.labs.remorph.intermediate.workflows.jobs.JobSettings
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

// see https://docs.databricks.com/en/dev-tools/bundles/settings.html
class GenerateBundleFile extends Rule[JobNode] with TransformationConstructors {
  private[this] val mapper =
    new ObjectMapper(new YAMLFactory())
      .setSerializationInclusion(Include.NON_DEFAULT)
      .registerModule(DefaultScalaModule)

  override def apply(tree: JobNode): Transformation[JobNode] = tree transform { case Migration(children) =>
    val resources = findResources(children)
    ok(Migration(children ++ Seq(bundleDefinition(resources))))
  }

  private def findResources(children: Seq[JobNode]): Resources = {
    var resources = Resources()
    children foreach {
      case schema: Schema =>
        resources = resources.withSchema(schema)
      case job: JobSettings =>
        resources = resources.withJob(job)
      case _ => // noop
    }
    resources
  }

  private def bundleDefinition(resources: Resources): CreatedFile = {
    val bundle = BundleFile(
      resources = Some(resources),
      bundle = Some(Bundle("remorphed")),
      targets =
        Map("dev" -> Target(mode = Some("development"), default = true), "prod" -> Target(mode = Some("production"))))
    val yml = mapper.writeValueAsString(bundle)
    CreatedFile("databricks.yml", yml)
  }
}
