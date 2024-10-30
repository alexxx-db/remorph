package com.databricks.labs.remorph.intermediate.workflows

import com.databricks.sdk.service.compute

case class RCranLibrary(spec: String, repo: Option[String] = None) extends JobNode {
  override def children: Seq[JobNode] = Seq()
  def toSDK: compute.RCranLibrary = {
    val raw = new compute.RCranLibrary()
    raw
  }
}