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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions._
import org.apache.flink.table.api.config.OptimizerConfigOptions._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

import java.time.Duration

class RemoveShuffleTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("T1", 'a1, 'b1, 'c1)
    util.addTableSource[(Int, Long, String)]("T2", 'a2, 'b2, 'c2)
    util.addTableSource[(Int, Long, String)]("T3", 'a3, 'b3, 'c3)
    util.addTableSource[(Int, Long, String)]("T4", 'a4, 'b4, 'c4)
    util.addTableSource[(Int, Long, String)](
      "T5",
      'a5,
      'b5,
      'c5,
      'proctime.proctime,
      'rowtime.rowtime())
    util.addTableSource[(Int, Long, String)](
      "T6",
      'a6,
      'b6,
      'c6,
      'proctime.proctime,
      'rowtime.rowtime())
    util.addTable("""
                    |CREATE TABLE T7 (
                    |  `a7` INT PRIMARY KEY NOT ENFORCED,
                    |  `b7` BIGINT,
                    |  `proctime` AS PROCTIME(),
                    |  `c7` VARCHAR
                    |) WITH (
                    |  'connector' = 'values',
                    |  'disable-lookup' = 'true'
                    |)
                    |""".stripMargin)
    util.addTable("""
                    |CREATE TABLE T8 (
                    |  `a8` INT PRIMARY KEY NOT ENFORCED,
                    |  `b8` BIGINT,
                    |  `c8` VARCHAR
                    |) WITH (
                    |  'connector' = 'values',
                    |  'disable-lookup' = 'true'
                    |)
                    |""".stripMargin)
    util.addTable("""
                    |CREATE TABLE L (
                    |  `id` INT,
                    |  `name` STRING,
                    |  `age` INT
                    |) WITH (
                    |  'connector' = 'values',
                    |  'lookup.cache' = 'LRU',
                    |  'lookup.cache.partitioned' = 'true'
                    |)
                    |""".stripMargin)
  }

  @Test
  def testMultipleInnerJoins(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4
         |WHERE a1 = a2 AND a2 = a3 AND a3 = a4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleInnerJoinsWithMultipleKeys(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4
         |WHERE a1 = a2 AND b1 = b2 AND a2 = a3 AND b3 = b2 AND a3 = a4 AND b3 = b4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleInnerJoinsWithPartialKey(): Unit = {
    // partial keys can not be pushed down
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4
         |WHERE a1 = a2 AND a2 = a3 AND a3 = a4 AND b3 = b4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleLeftJoins(): Unit = {
    val sql =
      s"""
         |SELECT * FROM
         | (SELECT * FROM
         |   (SELECT * FROM T1 LEFT JOIN T2 ON a1 = a2) TMP1
         |     LEFT JOIN T3 ON a1 = a3) TMP2
         | LEFT JOIN T4 ON a1 = a4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleLeftJoinsWithJoinKeyWithRightSide(): Unit = {
    // join key from outer side will not be pushed down
    val sql =
      s"""
         |SELECT * FROM
         |  (SELECT * FROM T1 LEFT JOIN T2 ON a1 = a2) TMP
         |    LEFT JOIN T3 ON a2 = a3
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleRightJoinsWithJoinKeyWithLeftSide(): Unit = {
    // join key from outer side will not be pushed down
    val sql =
      s"""
         |SELECT * FROM
         |  (SELECT * FROM T1 RIGHT JOIN T2 ON a1 = a2) TMP
         |    RIGHT JOIN T3 ON a1 = a3
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleFullJoins(): Unit = {
    // join key from outer side will not be pushed down
    val sql =
      s"""
         |SELECT * FROM
         |  (SELECT * FROM T1 FULL JOIN T2 ON a1 = a2) TMP
         |    FULL JOIN T3 ON a1 = a3
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleGroupAggWithEmptyKey(): Unit = {
    // singleton distribution will not be pushed down
    val sql =
      s"""
         |SELECT SUM(b) FROM (SELECT COUNT(b1) AS b FROM T1) t
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleGroupAggWithSameKeys(): Unit = {
    val sql =
      s"""
         |SELECT a1, count(*) FROM (SELECT a1 FROM T1 GROUP BY a1) t GROUP BY a1
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithAggs(): Unit = {
    val query1 = "SELECT a1, SUM(b1) AS b1 FROM T1 GROUP BY a1"
    val query2 = "SELECT a2, SUM(b2) AS b2 FROM T2 GROUP BY a2"
    val query = s"SELECT * FROM ($query1) LEFT JOIN ($query2) ON a1 = a2"
    util.verifyExecPlan(query)
  }

  @Test
  def testJoinWithAggsWithCalc(): Unit = {
    // Calc also support required distribution push down
    val query1 = "SELECT SUM(b1) AS b1, a1 FROM T1 GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, a2 FROM T2 GROUP BY a2"
    val query = s"SELECT * FROM ($query1) LEFT JOIN ($query2) ON a1 = a2"
    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithJoinFromLeftKeys(): Unit = {
    val sql =
      s"""
         |SELECT a1, b1, COUNT(a2), SUM(b2) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY a1, b1
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithJoinFromRightKeys(): Unit = {
    val sql =
      s"""
         |SELECT a2, b2, COUNT(a1), SUM(b1) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY b2, a2
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithJoinWithPartialKey(): Unit = {
    // partial keys can not be pushed down
    val sql =
      s"""
         |SELECT a1, MAX(b1), COUNT(a2), SUM(b2) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY a1
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithSemiJoin(): Unit = {
    val sql =
      s"""
         |SELECT a1, MAX(b1), COUNT(c1)
         |FROM T1
         |WHERE EXISTS
         |  (SELECT 1 FROM T2 WHERE T1.a1 = T2.a2)
         |GROUP BY a1
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithAntiJoin(): Unit = {
    val sql =
      s"""
         |SELECT a1, MAX(b1), COUNT(c1)
         |FROM T1
         |WHERE NOT EXISTS
         |  (SELECT 1 FROM T2 WHERE T1.a1 = T2.a2)
         |GROUP BY a1
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithDeduplicate(): Unit = {
    val query1 =
      s"""
         |SELECT a5, b5
         |FROM (
         |  SELECT a5, b5, ROW_NUMBER() OVER (PARTITION BY a5 ORDER BY proctime) as rank_num
         |  FROM T5
         |)
         |WHERE rank_num = 1
         |""".stripMargin
    val query2 =
      s"""
         |SELECT a6, b6
         |FROM (
         |  SELECT a6, b6, ROW_NUMBER() OVER (PARTITION BY a6 ORDER BY proctime) as rank_num
         |  FROM T6
         |)
         |WHERE rank_num = 1
         |""".stripMargin
    val query = s"SELECT * FROM ($query1) LEFT JOIN ($query2) ON a5 = a6"
    util.verifyExecPlan(query)
  }

  @Test
  def testJoinWithRank(): Unit = {
    val query1 =
      s"""
         |SELECT a5, b5
         |FROM (
         |  SELECT a5, b5, ROW_NUMBER() OVER (PARTITION BY a5 ORDER BY proctime) as rank_num
         |  FROM T5
         |)
         |WHERE rank_num = 5
         |""".stripMargin
    val query2 =
      s"""
         |SELECT a6, b6
         |FROM (
         |  SELECT a6, b6, ROW_NUMBER() OVER (PARTITION BY a6 ORDER BY proctime) as rank_num
         |  FROM T6
         |)
         |WHERE rank_num = 5
         |""".stripMargin
    val query = s"SELECT * FROM ($query1) LEFT JOIN ($query2) ON a5 = a6"
    util.verifyExecPlan(query)
  }

  @Test
  def testDeduplicateWithWindowAggregate(): Unit = {
    val query1 =
      """
        |SELECT
        |   a5,
        |   window_start,
        |   window_time,
        |   count(*) as cnt
        |FROM TABLE(TUMBLE(TABLE T5, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |GROUP BY a5, window_start, window_end, window_time
      """.stripMargin
    val query =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a5, cnt, ROW_NUMBER() OVER (PARTITION BY a5 ORDER BY window_time) as rank_num
         |  FROM ($query1)
         |)
         |WHERE rank_num = 1
         |""".stripMargin
    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithIntervalJoin(): Unit = {
    val sql =
      """
        |SELECT a5, MAX(b6) FROM
        |(
        |  SELECT a5, b6 FROM T5 JOIN T6 ON
        |   a5 = a6 AND T5.proctime BETWEEN T6.proctime - INTERVAL '5' SECOND AND T6.proctime
        |)
        |GROUP BY a5
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithTemporalJoinLeftSide(): Unit = {
    val sql =
      """
        |SELECT a7, MAX(b8) FROM
        |(
        |  SELECT a7, b8 FROM T7 JOIN T8 FOR SYSTEM_TIME AS OF T7.proctime as r1 ON
        |   a7 = a8
        |)
        |GROUP BY a7
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithTemporalJoinRightSide(): Unit = {
    val sql =
      """
        |SELECT a8, MAX(b7) FROM
        |(
        |  SELECT a8, b7 FROM T7 JOIN T8 FOR SYSTEM_TIME AS OF T7.proctime as r1 ON
        |   a7 = a8
        |)
        |GROUP BY a8
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithWindowDeduplicate(): Unit = {
    val query =
      s"""
         |SELECT a5, MAX(b5)
         |FROM (
         |  SELECT *
         |  FROM (
         |    SELECT *,
         |      ROW_NUMBER() OVER(PARTITION BY a5, window_start, window_end
         |      ORDER BY proctime DESC) as rownum
         |    FROM TABLE(TUMBLE(TABLE T5, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
         |)
         |WHERE rownum <= 1
         |)
         |GROUP BY a5
         |""".stripMargin
    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithWindowRank(): Unit = {
    val query =
      s"""
         |SELECT a5, MAX(b5)
         |FROM (
         |  SELECT *
         |  FROM (
         |    SELECT *,
         |      ROW_NUMBER() OVER(PARTITION BY a5, window_start, window_end
         |      ORDER BY proctime DESC) as rownum
         |    FROM TABLE(TUMBLE(TABLE T5, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
         |)
         |WHERE rownum <= 5
         |)
         |GROUP BY a5
         |""".stripMargin
    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithWindowJoin(): Unit = {
    val query1 =
      """
        |SELECT *
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE T5, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE b5 > 10
        |) L
        |JOIN (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE T6, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE b6 > 10
        |) R
        |ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.a5 = R.a6
        |""".stripMargin
    val query =
      s"""
         |SELECT a5, MAX(b6)
         |FROM ($query1)
         |GROUP BY a5
         |""".stripMargin
    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithOverAgg(): Unit = {
    val query =
      s"""
         |SELECT a5, MAX(maxc + minc)
         |FROM (
         |  SELECT a5,
         |    SUM(b5) OVER (PARTITION BY a5 ORDER BY proctime
         |       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as maxc,
         |    MIN(b5) OVER (PARTITION BY a5 ORDER BY proctime
         |       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as minc
         |  FROM T5
         |)
         |GROUP BY a5
         |""".stripMargin
    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithUnion(): Unit = {
    val query =
      """
        |SELECT a, SUM(b)
        |FROM
        |(
        | SELECT * FROM
        | (SELECT a1 a, MAX(b1) b FROM T1 GROUP BY a1)
        | UNION ALL
        | (SELECT a2 a, MAX(b2) b FROM T2 GROUP BY a2)
        |)
        |GROUP BY a
        |""".stripMargin

    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithCorrelate(): Unit = {
    util.addFunction("str_split", new StringSplit())
    val query =
      """
        |SELECT a1, MAX(d)
        |FROM
        |(
        | SELECT * FROM
        | (
        |   SELECT a1, FIRST_VALUE(c1) as c1 FROM T1 GROUP BY a1
        | )
        | , LATERAL TABLE(str_split(c1, ',')) as T0(d)
        |)
        |GROUP BY a1
        |""".stripMargin

    util.verifyExecPlan(query)
  }

  @Test
  def testMiniBatchGroupAggWithSameKeys(): Unit = {
    val tableConfig = util.getTableEnv.getConfig.getConfiguration
    tableConfig.set(TABLE_EXEC_MINIBATCH_ENABLED, Boolean.box(true))
    tableConfig.set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    tableConfig.set(TABLE_EXEC_MINIBATCH_SIZE, Long.box(3))
    val sql =
      s"""
         |SELECT a1, count(*) FROM (SELECT a1 FROM T1 GROUP BY a1) t GROUP BY a1
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testSplitGroupAggWithSameKeys(): Unit = {
    val tableConfig = util.getTableEnv.getConfig.getConfiguration
    tableConfig.set(TABLE_EXEC_MINIBATCH_ENABLED, Boolean.box(true))
    tableConfig.set(TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofSeconds(1))
    tableConfig.set(TABLE_EXEC_MINIBATCH_SIZE, Long.box(3))
    tableConfig.set(TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
    val sql =
      s"""
         |SELECT a1, MAX(b1) FROM (SELECT a1, COUNT(DISTINCT b1) as b1 FROM T1 GROUP BY a1) t GROUP BY a1
         |""".stripMargin
    util.verifyExecPlan(sql)
  }
}
