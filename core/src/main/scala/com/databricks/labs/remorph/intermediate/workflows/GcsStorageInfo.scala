package com.databricks.labs.remorph.intermediate.workflows

import com.databricks.sdk.service.compute

case class GcsStorageInfo(destination: String) extends JobNode {
  override def children: Seq[JobNode] = Seq()
  def toSDK: compute.GcsStorageInfo = new compute.GcsStorageInfo().setDestination(destination)
}