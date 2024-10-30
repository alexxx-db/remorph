package com.databricks.labs.remorph.intermediate.workflows

import com.databricks.sdk.service.jobs
import com.databricks.sdk.service.jobs.PeriodicTriggerConfigurationTimeUnit

case class PeriodicTriggerConfiguration(interval: Int, unit: PeriodicTriggerConfigurationTimeUnit) extends JobNode {
  override def children: Seq[JobNode] = Seq()
  def toSDK: jobs.PeriodicTriggerConfiguration = {
    val raw = new jobs.PeriodicTriggerConfiguration()
    raw
  }
}