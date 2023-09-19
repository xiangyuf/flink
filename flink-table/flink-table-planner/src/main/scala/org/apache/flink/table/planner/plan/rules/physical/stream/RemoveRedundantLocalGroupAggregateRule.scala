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

import org.apache.flink.table.planner.plan.nodes.physical.stream.{StreamPhysicalGlobalGroupAggregate, StreamPhysicalGroupAggregate, StreamPhysicalLocalGroupAggregate}

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.RelOptRule.{any, operand}

/**
 * There maybe exist a subTree like [[StreamPhysicalLocalGroupAggregate]] ->
 * [[StreamPhysicalGlobalGroupAggregate]] which the middle shuffle is removed. The rule could
 * replace the given subTree with a [[StreamPhysicalGroupAggregate]].
 */
class RemoveRedundantLocalGroupAggregateRule
  extends RelOptRule(
    operand(
      classOf[StreamPhysicalGlobalGroupAggregate], // global agg
      operand(
        classOf[StreamPhysicalLocalGroupAggregate], // local agg
        any())
    ),
    "RemoveRedundantLocalGroupAggregateRule") {

  override def onMatch(call: RelOptRuleCall): Unit = {
    val globalAgg: StreamPhysicalGlobalGroupAggregate = call.rel(0)
    val localAgg: StreamPhysicalLocalGroupAggregate = call.rel(1)
    val newAgg = new StreamPhysicalGroupAggregate(
      globalAgg.getCluster,
      globalAgg.getTraitSet,
      localAgg.getInput,
      globalAgg.getRowType,
      localAgg.grouping,
      localAgg.aggCalls,
      globalAgg.partialFinalType)
    call.transformTo(newAgg)
  }
}

object RemoveRedundantLocalGroupAggregateRule {
  val INSTANCE = new RemoveRedundantLocalGroupAggregateRule
}
