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
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalJoin

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.{RelDistribution, RelNode}
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.ImmutableIntList

import scala.collection.JavaConversions._
import scala.collection.mutable

/** Base Stream physical RelNode for [[Join]] */
abstract class StreamPhysicalJoinBase(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType)
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel {

  override def satisfyTraits(requiredTraitSet: RelTraitSet): Option[RelNode] = {
    val requiredDistribution = requiredTraitSet.getTrait(FlinkRelDistributionTraitDef.INSTANCE)
    if (requiredDistribution.getType != RelDistribution.Type.HASH_DISTRIBUTED) {
      return None
    }
    // Full outer join cannot provide Hash distribute because it will generate null for left/right
    // side if there is no match row.
    if (joinType == JoinRelType.FULL) {
      return None
    }

    val leftKeys = joinInfo.leftKeys
    val rightKeys = joinInfo.rightKeys
    if (leftKeys.isEmpty || rightKeys.isEmpty) {
      return None
    }

    val leftFieldCnt = getLeft.getRowType.getFieldCount
    val requiredShuffleKeys = requiredDistribution.getKeys
    val requiredLeftShuffleKeyBuffer = mutable.ArrayBuffer[Int]()
    val requiredRightShuffleKeyBuffer = mutable.ArrayBuffer[Int]()
    // SEMI and ANTI Join can only be push down to left side.
    requiredShuffleKeys.foreach {
      key =>
        if (
          key < leftFieldCnt &&
          (joinType == JoinRelType.LEFT || joinType == JoinRelType.INNER
            || joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI)
        ) {
          requiredLeftShuffleKeyBuffer += key
        } else if (
          key >= leftFieldCnt &&
          (joinType == JoinRelType.RIGHT || joinType == JoinRelType.INNER)
        ) {
          requiredRightShuffleKeyBuffer += (key - leftFieldCnt)
        } else {
          // cannot satisfy required hash distribution if requirement shuffle keys are not come from
          // left side when Join is LOJ or are not come from right side when Join is ROJ.
          return None
        }
    }

    val requiredLeftShuffleKeys = ImmutableIntList.of(requiredLeftShuffleKeyBuffer: _*)
    val requiredRightShuffleKeys = ImmutableIntList.of(requiredRightShuffleKeyBuffer: _*)
    // the required hash distribution can be pushed down
    // only if the required keys are all from one side and totally equal to the side's keys
    if (
      leftKeys.equals(requiredLeftShuffleKeys) && requiredRightShuffleKeys.isEmpty ||
      rightKeys.equals(requiredRightShuffleKeys) && requiredLeftShuffleKeys.isEmpty
    ) {
      val providedTraits = getTraitSet.replace(requiredDistribution)
      Some(copy(providedTraits, Seq(getLeft, getRight)))
    } else {
      None
    }
  }

}
