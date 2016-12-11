/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

@DeveloperApi
case class RangeJoin(left: SparkPlan,
                     right: SparkPlan,
                     condition: Seq[Expression],
                     context: SQLContext) extends BinaryNode with Serializable {
  def output: Seq[Attribute] = left.output ++ right.output

  lazy val (leftKeys, rightKeys) = condition.length match {
  // distinguish between "overlaps" and "genomeoverlap" mode
    case 4 => (List (condition (0), condition (1) ), List (condition (2), condition (3) ) )
    case 6 => (List (condition (0), condition (1), condition (2)),
      List (condition (3), condition (4), condition (5)))
    case _ => throw new RuntimeException("Unknown condition length")
  }

  lazy val (buildPlan, buildKeys, streamPlan, streamKeys) = (left, leftKeys, right, rightKeys)

  @transient lazy val buildKeyGenerator = new InterpretedProjection(buildKeys, left.output)
  @transient lazy val streamKeyGenerator = new InterpretedProjection(streamKeys,
    right.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val v1 = buildPlan.execute()
    val v1kv = v1.map(x => {
      val v1Key = buildKeyGenerator(x)
      (new Interval[Long](v1Key.getLong(0), v1Key.getLong(1),
        maybeGetThirdKey(v1Key)),
        x.copy())
    } )
    val v2 = streamPlan.execute()
    val v2kv = v2.map(x => {
      val v2Key = streamKeyGenerator(x)
      (new Interval[Long](v2Key.getLong(0), v2Key.getLong(1),
        maybeGetThirdKey(v2Key)),
        x.copy())
    } )
    /* As we are going to collect v1 and build an interval tree on its intervals,
    make sure that its size is the smaller one. */
    assert(v1.count <= v2.count)
    val v3 = RangeJoinImpl.overlapJoin(context.sparkContext, v1kv, v2kv)
      .flatMap(l => l._2.map(r => (l._1, r)))
    val v4 = v3.map {
      case (l: InternalRow, r: InternalRow) => new JoinedRow(l, r).withLeft(l).copy()
    }
    v4
  }

  def maybeGetThirdKey(r: InternalRow): Option[String] = r.numFields match {
    case 3 => Some(r.getString(2))
    case _ => None
  }

}

/* intervals with different groupId values do not overlap */
case class Interval[T <% Long](start: T, end: T, groupId: Option[String] = None) {
  def overlaps(other: Interval[T]): Boolean = {
    (groupId == other.groupId) &&
    (end >= start) && (other.end >= other.start) &&
      (end > other.start && start < other.end)
  }
}
