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
import org.apache.flink.table.planner.plan.nodes.common.CommonCalc
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil

import org.apache.calcite.plan.{RelOptCluster, RelOptRule, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelDistribution, RelNode}
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram

import scala.collection.JavaConversions._

/** Base stream physical RelNode for [[Calc]]. */
abstract class StreamPhysicalCalcBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    calcProgram: RexProgram,
    outputRowType: RelDataType)
  extends CommonCalc(cluster, traitSet, inputRel, calcProgram)
  with StreamPhysicalRel {

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    if (requiredDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return None
    }

    val mapping = FlinkRexUtil.getProjectMapping(getInput.getRowType.getFieldCount, calcProgram)
    val appliedDistribution = requiredDistribution.apply(mapping)

    if (appliedDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return None
    }

    val inputRequiredTraits = getInput.getTraitSet.replace(appliedDistribution)
    val providedTraits = getTraitSet.replace(requiredDistribution)
    val newInput = RelOptRule.convert(getInput, inputRequiredTraits)
    Some(copy(providedTraits, Seq(newInput)))
  }

  override def requireWatermark: Boolean = false

  override def deriveRowType(): RelDataType = outputRowType

}
