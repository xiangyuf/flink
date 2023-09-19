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
package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.configuration.ReadableConfig
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalGlobalGroupAggregate, StreamPhysicalGroupAggregate, StreamPhysicalLocalGroupAggregate, StreamPhysicalRel}
import org.apache.flink.table.planner.plan.utils.{AggregateInfo, AggregateUtil, WindowUtil}
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate.Group

import scala.collection.JavaConversions._

/**
 * Rule that matches [[FlinkLogicalAggregate]] which all aggregate function buffer are fix length,
 * and converts it to
 * {{{
 *   StreamPhysicalGlobalGroupAggregate
 *   +- StreamPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *      +- StreamPhysicalLocalGroupAggregate
 *         +- input of agg
 * }}}
 * when 1. all aggregate functions are mergeable, 2. the input distribution cannot satisfy the
 * distribution trait of the aggregate, 3. mini-batch is enabled in given TableConfig 4.
 * [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is TWO_PHASE, or
 * {{{
 *   StreamPhysicalGroupAggregate
 *   +- StreamPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *      +- input of agg
 * }}}
 * when some aggregate functions are not mergeable or
 * [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is ONE_PHASE.
 *
 * Notes: if [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is AUTO, this rule will
 * try to converts it using TWO_PHASE, if the conditions cannot be satified then choose ONE_PHASE.
 * It's different from the behaviors in batch as the cost model for streaming is not fully refined.
 * Therefore we cannot decide which it is best based on cost.
 */
class StreamPhysicalGroupAggregateRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalAggregate], operand(classOf[RelNode], any)),
    "StreamPhysicalGroupAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalAggregate = call.rel(0)

    // check if we have grouping sets
    if (agg.getGroupType != Group.SIMPLE || agg.indicator) {
      throw new TableException("GROUPING SETS are currently not supported.")
    }

    if (agg.getAggCallList.exists(isPythonAggregate(_))) {
      return false
    }

    // check not window aggregate
    val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
    val windowProperties = fmq.getRelWindowProperties(agg.getInput)
    val grouping = agg.getGroupSet
    !WindowUtil.groupingContainsWindowStartEnd(grouping, windowProperties)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = unwrapTableConfig(call)
    val originAgg: FlinkLogicalAggregate = call.rel(0)
    val originAggCalls = originAgg.getAggCallList
    val originGrouping = originAgg.getGroupSet.toArray
    val providedTraitSet = originAgg.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val input: RelNode = call.rel(1)
    val isInputSatisfyRequiredDistribution =
      AggregateUtil.isInputSatisfyRequiredDistribution(input, originGrouping)
    val aggInfoList =
      AggregateUtil.deriveAggregateInfoList(originAgg, originAgg.getGroupCount, originAggCalls)
    if (
      !isInputSatisfyRequiredDistribution &&
      isTwoPhaseAggWorkable(aggInfoList.aggInfos, tableConfig)
    ) {
      // FlinkChangelogModeInferenceProgram is not applied yet, false as default
      val needRetraction = false
      val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
      val monotonicity = fmq.getRelModifiedMonotonicity(originAgg)
      val aggCallNeedRetractions = AggregateUtil.deriveAggCallNeedRetractions(
        originAgg.getGroupCount,
        originAggCalls,
        needRetraction,
        monotonicity)

      val localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
      val localInput = RelOptRule.convert(input, localRequiredTraitSet)
      val localAgg = new StreamPhysicalLocalGroupAggregate(
        originAgg.getCluster,
        providedTraitSet,
        localInput,
        originGrouping,
        originAggCalls,
        aggCallNeedRetractions,
        needRetraction,
        originAgg.partialFinalType)

      // grouping keys is forwarded by local agg, use indices instead of groupings
      val globalGrouping = originGrouping.indices.toArray
      val globalDistribution = AggregateUtil.createDistribution(globalGrouping)
      val globalRequiredTraitSet = localAgg.getTraitSet.replace(globalDistribution)
      val globalInput = RelOptRule.convert(localAgg, globalRequiredTraitSet)

      val globalAgg = new StreamPhysicalGlobalGroupAggregate(
        originAgg.getCluster,
        providedTraitSet,
        globalInput,
        originAgg.getRowType,
        globalGrouping,
        originAggCalls,
        aggCallNeedRetractions,
        input.getRowType,
        needRetraction,
        originAgg.partialFinalType)

      call.transformTo(globalAgg)
    } else {
      val newInput = if (isInputSatisfyRequiredDistribution) {
        input
      } else {
        val requiredDistribution =
          AggregateUtil.createDistribution(originGrouping)
        val requiredTraitSet = originAgg.getCluster.getPlanner
          .emptyTraitSet()
          .replace(requiredDistribution)
          .replace(FlinkConventions.STREAM_PHYSICAL)
        RelOptRule.convert(originAgg.getInput, requiredTraitSet)
      }
      val newAgg = new StreamPhysicalGroupAggregate(
        originAgg.getCluster,
        providedTraitSet,
        newInput,
        originAgg.getRowType,
        originAgg.getGroupSet.toArray,
        originAgg.getAggCallList,
        originAgg.partialFinalType)

      call.transformTo(newAgg)
    }
  }

  protected def isTwoPhaseAggWorkable(
      aggInfos: Array[AggregateInfo],
      tableConfig: ReadableConfig): Boolean = {
    val isMiniBatchEnabled = tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
    getAggPhaseStrategy(tableConfig) match {
      case AggregatePhaseStrategy.ONE_PHASE => false
      case _ => AggregateUtil.doAllSupportPartialMerge(aggInfos) && isMiniBatchEnabled
    }
  }
}

object StreamPhysicalGroupAggregateRule {
  val INSTANCE = new StreamPhysicalGroupAggregateRule()
}
