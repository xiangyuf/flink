/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan
import org.apache.flink.table.planner.plan.utils.{FlinkRexUtil, RelExplainUtil}

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelDistribution, RelNode, RelWriter, SingleRel}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.{RexCall, RexNode}

import scala.collection.JavaConversions._

/** Base Flink RelNode which matches along with join a user defined table function. */
abstract class StreamPhysicalCorrelateBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    val inputRel: RelNode,
    val scan: FlinkLogicalTableFunctionScan,
    val condition: Option[RexNode],
    outputRowType: RelDataType,
    joinType: JoinRelType)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel {

  require(joinType == JoinRelType.INNER || joinType == JoinRelType.LEFT)

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    copy(traitSet, inputs.get(0), outputRowType)
  }

  /** Note: do not passing member 'child' because singleRel.replaceInput may update 'input' rel. */
  def copy(traitSet: RelTraitSet, newChild: RelNode, outputType: RelDataType): RelNode

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    if (requiredDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return None
    }

    val mapping = FlinkRexUtil.getOutputInputMapping(getInput.getRowType.getFieldCount)
    val appliedDistribution = requiredDistribution.apply(mapping)

    if (appliedDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return None
    }

    val inputRequiredTraits = getInput.getTraitSet.replace(appliedDistribution)
    val providedTraits = getTraitSet.replace(requiredDistribution)
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    Some(copy(providedTraits, Seq(newInput)))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val rexCall = scan.getCall.asInstanceOf[RexCall]
    super
      .explainTerms(pw)
      .item("invocation", scan.getCall)
      .item(
        "correlate",
        RelExplainUtil.correlateToString(
          inputRel.getRowType,
          rexCall,
          getExpressionString,
          convertToExpressionDetail(pw.getDetailLevel)))
      .item("select", outputRowType.getFieldNames.mkString(","))
      .item("rowType", outputRowType)
      .item("joinType", joinType)
      .itemIf("condition", condition.orNull, condition.isDefined)
  }

}
