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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, FlinkRelDistributionTraitDef}
import org.apache.flink.table.planner.plan.PartialFinalType
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecLocalGroupAggregate
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.utils.ShortcutUtils.{unwrapTableConfig, unwrapTypeFactory}

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.util.ImmutableIntList

import java.util

import scala.collection.JavaConversions._

/**
 * Stream physical RelNode for unbounded local group aggregate.
 *
 * @see
 *   [[StreamPhysicalGroupAggregateBase]] for more info.
 */
class StreamPhysicalLocalGroupAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    grouping: Array[Int],
    aggCalls: Seq[AggregateCall],
    aggCallNeedRetractions: Array[Boolean],
    needRetraction: Boolean,
    val partialFinalType: PartialFinalType)
  extends StreamPhysicalGroupAggregateBase(cluster, traitSet, inputRel, grouping, aggCalls) {

  private lazy val aggInfoList = AggregateUtil.transformToStreamAggregateInfoList(
    unwrapTypeFactory(inputRel),
    FlinkTypeFactory.toLogicalRowType(inputRel.getRowType),
    aggCalls,
    aggCallNeedRetractions,
    needRetraction,
    isStateBackendDataViews = false
  )

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = {
    AggregateUtil.inferLocalAggRowType(
      aggInfoList,
      inputRel.getRowType,
      grouping,
      getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory])
  }

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamPhysicalLocalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      grouping,
      aggCalls,
      aggCallNeedRetractions,
      needRetraction,
      partialFinalType)
  }

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    val canSatisfy = requiredDistribution.getType match {
      case HASH_DISTRIBUTED =>
        val shuffleKeys = requiredDistribution.getKeys
        val groupKeysList = ImmutableIntList.of(grouping.indices.toArray: _*)
        /*
         * As local agg is the input of global agg, the required distribution can be of two kinds:
         * 1. distribution that can be satisfied by global agg;
         * 2. distribution that required by global agg from StreamPhysicalGroupAggregateRule.
         * Local agg can only satisfy type 1 now. If type 2 is satisfied, local-global agg will
         * become a normal agg after RemoveRedundantLocalGroupAggregateRule is applied.
         * The cost model may choose the normal one in this case.
         * Todo: refine the cost model to choose the best plan in this case.
         */
        grouping.nonEmpty && shuffleKeys == groupKeysList
      case _ => false
    }
    if (!canSatisfy) {
      return None
    }

    val inputRequiredDistribution = FlinkRelDistribution.hash(grouping, requireStrict = true)
    val inputRequiredTraits = input.getTraitSet.replace(inputRequiredDistribution)
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    val providedTraits = getTraitSet.replace(requiredDistribution)
    Some(copy(providedTraits, Seq(newInput)))
  }

  def updateNeedRetraction(
      inputs: util.List[RelNode],
      needRetraction: Boolean,
      aggCallNeedRetractions: Array[Boolean]): StreamPhysicalLocalGroupAggregate = {
    new StreamPhysicalLocalGroupAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      grouping,
      aggCalls,
      aggCallNeedRetractions,
      needRetraction,
      partialFinalType)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = getInput.getRowType
    super
      .explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .itemIf("partialFinalType", partialFinalType, partialFinalType != PartialFinalType.NONE)
      .item(
        "select",
        RelExplainUtil.streamGroupAggregationToString(
          inputRowType,
          getRowType,
          aggInfoList,
          grouping,
          isLocal = true))
  }

  override def translateToExecNode(): ExecNode[_] = {
    new StreamExecLocalGroupAggregate(
      unwrapTableConfig(this),
      grouping,
      aggCalls.toArray,
      aggCallNeedRetractions,
      needRetraction,
      InputProperty.DEFAULT,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
