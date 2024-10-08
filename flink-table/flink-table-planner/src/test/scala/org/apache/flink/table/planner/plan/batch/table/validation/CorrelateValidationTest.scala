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
package org.apache.flink.table.planner.plan.batch.table.validation

import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{TableFunc1, TableTestBase}

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class CorrelateValidationTest extends TableTestBase {

  /**
   * Due to the improper translation of TableFunction left outer join (see CALCITE-2004), the join
   * predicate can only be empty or literal true (the restriction should be removed in FLINK-7865).
   */
  @Test
  def testLeftOuterJoinWithPredicates(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val func = new TableFunc1

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => {
          val result = table
            .leftOuterJoinLateral(func('c).as('s), 'c === 's)
            .select('c, 's)
          util.verifyExecPlan(result)
        })
  }
}
