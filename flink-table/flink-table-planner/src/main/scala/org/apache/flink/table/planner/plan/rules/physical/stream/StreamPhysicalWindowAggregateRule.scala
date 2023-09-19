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
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.logical.{WindowAttachedWindowingStrategy, WindowingStrategy}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate
import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalCalc, StreamPhysicalGlobalWindowAggregate, StreamPhysicalLocalWindowAggregate, StreamPhysicalWindowAggregate, StreamPhysicalWindowAggregateBase}
import org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalWindowAggregateRule.{WINDOW_END, WINDOW_START, WINDOW_TIME}
import org.apache.flink.table.planner.plan.utils.{AggregateInfo, AggregateInfoList, AggregateUtil, WindowUtil}
import org.apache.flink.table.planner.plan.utils.PythonUtil.isPythonAggregate
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy
import org.apache.flink.table.planner.utils.ShortcutUtils.{unwrapTableConfig, unwrapTypeFactory}
import org.apache.flink.table.planner.utils.TableConfigUtils.getAggPhaseStrategy
import org.apache.flink.table.runtime.groupwindow._

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Aggregate.Group
import org.apache.calcite.rex.{RexInputRef, RexProgram}

import java.util.stream.IntStream

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Rule that matches [[FlinkLogicalAggregate]] and converts it to
 * {{{
 *   StreamPhysicalGlobalWindowAggregate
 *   +- StreamPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *      +- StreamPhysicalLocalWindowAggregate
 *         +- input of agg
 * }}}
 * when <ul> <li>the applied windowing is not on processing-time, because processing-time should be
 * materialized in a single node. <li>[[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]]
 * is TWO_PHASE or AUTO. <li>all aggregate functions support merge() method. <li>the input of
 * exchange does not satisfy the shuffle distribution </ul> or
 * {{{
 *   StreamPhysicalWindowAggregate
 *   +- StreamPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *      +- input of agg
 * }}}
 * when [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is ONE_PHASE or any of the
 * above conditions is not satisfied.
 *
 * Notes: if [[OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY]] is AUTO, this rule will
 * try to converts it using TWO_PHASE, if the conditions cannot be satified then choose ONE_PHASE.
 * It's different from the behaviors in batch as the cost model for streaming is not fully refined.
 * Therefore we cannot decide which it is best based on cost.
 */
class StreamPhysicalWindowAggregateRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalAggregate], operand(classOf[RelNode], any)),
    "StreamPhysicalWindowAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: FlinkLogicalAggregate = call.rel(0)

    // check if we have grouping sets
    if (agg.getGroupType != Group.SIMPLE || agg.indicator) {
      throw new TableException("GROUPING SETS are currently not supported.")
    }

    // the aggregate calls shouldn't contain python aggregates
    if (agg.getAggCallList.asScala.exists(isPythonAggregate(_))) {
      return false
    }

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(call.getMetadataQuery)
    val windowProperties = fmq.getRelWindowProperties(agg.getInput)
    val grouping = agg.getGroupSet
    WindowUtil.groupingContainsWindowStartEnd(grouping, windowProperties)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val tableConfig = unwrapTableConfig(call)
    val originAgg: FlinkLogicalAggregate = call.rel(0)
    val input: RelNode = call.rel(1)
    val originAggCalls = originAgg.getAggCallList
    val originGrouping = originAgg.getGroupSet
    val providedTraitSet = originAgg.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val fmq = FlinkRelMetadataQuery.reuseOrCreate(originAgg.getCluster.getMetadataQuery)
    val relWindowProperties = fmq.getRelWindowProperties(originAgg.getInput)
    // we have check there is only one start and end in groupingContainsWindowStartEnd()
    val (startColumns, endColumns, timeColumns, newGrouping) =
      WindowUtil.groupingExcludeWindowStartEndTimeColumns(originGrouping, relWindowProperties)

    val windowingStrategy = new WindowAttachedWindowingStrategy(
      relWindowProperties.getWindowSpec,
      relWindowProperties.getTimeAttributeType,
      startColumns.toArray.head,
      endColumns.toArray.head)

    val windowProperties = createPlannerNamedWindowProperties(
      windowingStrategy,
      startColumns.toArray,
      endColumns.toArray,
      timeColumns.toArray)

    val aggInfoList: AggregateInfoList = AggregateUtil.deriveStreamWindowAggregateInfoList(
      unwrapTypeFactory(input),
      FlinkTypeFactory.toLogicalRowType(input.getRowType),
      originAggCalls.asScala,
      windowingStrategy.getWindow,
      isStateBackendDataViews = true
    )
    val isInputSatisfyRequiredDistribution =
      AggregateUtil.isInputSatisfyRequiredDistribution(input, newGrouping.toArray)

    // step-1: build window aggregate node
    val windowAgg =
      if (
        !isInputSatisfyRequiredDistribution &&
        isTwoPhaseAggWorkable(windowingStrategy, aggInfoList.aggInfos, tableConfig)
      ) {
        val localRequiredTraitSet = input.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
        val localInput = RelOptRule.convert(input, localRequiredTraitSet)
        val localAgg = new StreamPhysicalLocalWindowAggregate(
          originAgg.getCluster,
          providedTraitSet,
          localInput,
          newGrouping.toArray,
          originAggCalls.asScala,
          windowingStrategy)

        // grouping keys is forwarded by local agg, use indices instead of groupings
        val globalGrouping = IntStream.range(0, newGrouping.toArray.length).toArray
        val globalDistribution = AggregateUtil.createDistribution(globalGrouping)
        // Add exchange instead of adding converter here. As the cost model of streaming
        // cannot make good decisions yet between local global window aggregates and normal
        // window aggregate.
        // Todo: add converter here to let the cost model decides whether to use two stage.
        val newInput = FlinkExpandConversionRule.satisfyDistribution(
          FlinkConventions.STREAM_PHYSICAL,
          localAgg,
          globalDistribution)

        // we put windowEnd at the end of local output fields
        val endIndex = localAgg.getRowType.getFieldCount - 1
        val globalWindowing = new WindowAttachedWindowingStrategy(
          windowingStrategy.getWindow,
          windowingStrategy.getTimeAttributeType,
          endIndex)

        new StreamPhysicalGlobalWindowAggregate(
          originAgg.getCluster,
          providedTraitSet,
          newInput,
          input.getRowType,
          globalGrouping,
          originAggCalls.asScala,
          globalWindowing,
          windowProperties)
      } else {
        val newInput = if (isInputSatisfyRequiredDistribution) {
          input
        } else {
          val requiredDistribution =
            AggregateUtil.createDistribution(newGrouping.toArray)
          val requiredTraitSet = originAgg.getCluster.getPlanner
            .emptyTraitSet()
            .replace(requiredDistribution)
            .replace(FlinkConventions.STREAM_PHYSICAL)
          RelOptRule.convert(originAgg.getInput, requiredTraitSet)
        }

        new StreamPhysicalWindowAggregate(
          originAgg.getCluster,
          providedTraitSet,
          newInput,
          newGrouping.toArray,
          originAgg.getAggCallList.asScala,
          windowingStrategy,
          windowProperties)
      }

    // step-2: build projection on window aggregate to fix the fields mapping
    val calc = buildCalcProjection(
      originGrouping.toArray,
      newGrouping.toArray,
      startColumns.toArray,
      endColumns.toArray,
      timeColumns.toArray,
      originAgg,
      windowAgg
    )

    call.transformTo(calc)
  }

  protected def isTwoPhaseAggWorkable(
      window: WindowingStrategy,
      aggInfos: Array[AggregateInfo],
      tableConfig: ReadableConfig): Boolean = getAggPhaseStrategy(tableConfig) match {
    case AggregatePhaseStrategy.ONE_PHASE => false
    // processing time window doesn't support two-phase,
    // otherwise the processing-time can't be materialized in a single node
    case _ => AggregateUtil.doAllSupportPartialMerge(aggInfos) && window.isRowtime
  }

  private def buildCalcProjection(
      grouping: Array[Int],
      newGrouping: Array[Int],
      startColumns: Array[Int],
      endColumns: Array[Int],
      timeColumns: Array[Int],
      agg: FlinkLogicalAggregate,
      windowAgg: StreamPhysicalWindowAggregateBase): StreamPhysicalCalc = {
    val projectionMapping = getProjectionMapping(
      grouping,
      newGrouping,
      startColumns,
      endColumns,
      timeColumns,
      windowAgg.namedWindowProperties,
      agg.getAggCallList.size())
    val projectExprs = projectionMapping.map(RexInputRef.of(_, windowAgg.getRowType))
    val calcProgram = RexProgram.create(
      windowAgg.getRowType,
      projectExprs.toList.asJava,
      null, // no filter
      agg.getRowType,
      agg.getCluster.getRexBuilder
    )
    val traitSet: RelTraitSet = agg.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)
    val newInput: RelNode = RelOptRule.convert(windowAgg, FlinkConventions.STREAM_PHYSICAL)

    new StreamPhysicalCalc(
      agg.getCluster,
      traitSet,
      newInput,
      calcProgram,
      calcProgram.getOutputRowType)
  }

  private def createPlannerNamedWindowProperties(
      windowingStrategy: WindowingStrategy,
      startColumns: Array[Int],
      endColumns: Array[Int],
      timeColumns: Array[Int]): Seq[NamedWindowProperty] = {
    val windowProperties = ArrayBuffer[NamedWindowProperty]()
    val windowRef = new WindowReference("w$", windowingStrategy.getTimeAttributeType)
    if (!startColumns.isEmpty) {
      windowProperties +=
        new NamedWindowProperty(WINDOW_START, new WindowStart(windowRef))
    }
    if (!endColumns.isEmpty) {
      windowProperties +=
        new NamedWindowProperty(WINDOW_END, new WindowEnd(windowRef))
    }
    if (!timeColumns.isEmpty) {
      val property = if (windowingStrategy.isRowtime) {
        new RowtimeAttribute(windowRef)
      } else {
        new ProctimeAttribute(windowRef)
      }
      windowProperties += new NamedWindowProperty(WINDOW_TIME, property)
    }
    windowProperties
  }

  private def getProjectionMapping(
      grouping: Array[Int],
      newGrouping: Array[Int],
      startColumns: Array[Int],
      endColumns: Array[Int],
      timeColumns: Array[Int],
      windowProperties: Seq[NamedWindowProperty],
      aggCount: Int): Array[Int] = {
    val (startPos, endPos, timePos) =
      windowPropertyPositions(windowProperties, newGrouping, aggCount)
    val keyMapping = grouping.map {
      key =>
        if (newGrouping.contains(key)) {
          newGrouping.indexOf(key)
        } else if (startColumns.contains(key)) {
          startPos
        } else if (endColumns.contains(key)) {
          endPos
        } else if (timeColumns.contains(key)) {
          timePos
        } else {
          throw new IllegalArgumentException(
            s"Can't find grouping key $$$key, this should never happen.")
        }
    }
    val aggMapping = (0 until aggCount).map(aggIndex => newGrouping.length + aggIndex)
    keyMapping ++ aggMapping
  }

  private def windowPropertyPositions(
      windowProperties: Seq[NamedWindowProperty],
      newGrouping: Array[Int],
      aggCount: Int): (Int, Int, Int) = {
    val windowPropsIndexOffset = newGrouping.length + aggCount
    var startPos = -1
    var endPos = -1
    var timePos = -1
    windowProperties.zipWithIndex.foreach {
      case (p, idx) =>
        if (WINDOW_START.equals(p.getName)) {
          startPos = windowPropsIndexOffset + idx
        } else if (WINDOW_END.equals(p.getName)) {
          endPos = windowPropsIndexOffset + idx
        } else if (WINDOW_TIME.equals(p.getName)) {
          timePos = windowPropsIndexOffset + idx
        }
    }
    (startPos, endPos, timePos)
  }
}

object StreamPhysicalWindowAggregateRule {
  val INSTANCE = new StreamPhysicalWindowAggregateRule()

  private val WINDOW_START: String = "window_start"
  private val WINDOW_END: String = "window_end"
  private val WINDOW_TIME: String = "window_time"
}
