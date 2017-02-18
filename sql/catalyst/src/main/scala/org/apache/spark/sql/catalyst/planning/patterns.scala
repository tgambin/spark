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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNodeRef

/**
 * A pattern that matches any number of project or filter operations on top of another relational
 * operator.  All filter operators are collected and their conditions are broken up and returned
 * together with the top project operator.
 * [[org.apache.spark.sql.catalyst.expressions.Alias Aliases]] are in-lined/substituted if
 * necessary.
 */
object PhysicalOperation extends PredicateHelper {
  type ReturnType = (Seq[NamedExpression], Seq[Expression], LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    val (fields, filters, child, _) = collectProjectsAndFilters(plan)
    Some((fields.getOrElse(child.output), filters, child))
  }

  /**
   * Collects all deterministic projects and filters, in-lining/substituting aliases if necessary.
   * Here are two examples for alias in-lining/substitution.
   * Before:
   * {{{
   *   SELECT c1 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   *   SELECT c1 AS c2 FROM (SELECT key AS c1 FROM t1) t2 WHERE c1 > 10
   * }}}
   * After:
   * {{{
   *   SELECT key AS c1 FROM t1 WHERE key > 10
   *   SELECT key AS c2 FROM t1 WHERE key > 10
   * }}}
   */
  private def collectProjectsAndFilters(plan: LogicalPlan):
      (Option[Seq[NamedExpression]], Seq[Expression], LogicalPlan, Map[Attribute, Expression]) =
    plan match {
      case Project(fields, child) if fields.forall(_.deterministic) =>
        val (_, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedFields = fields.map(substitute(aliases)).asInstanceOf[Seq[NamedExpression]]
        (Some(substitutedFields), filters, other, collectAliases(substitutedFields))

      case Filter(condition, child) if condition.deterministic =>
        val (fields, filters, other, aliases) = collectProjectsAndFilters(child)
        val substitutedCondition = substitute(aliases)(condition)
        (fields, filters ++ splitConjunctivePredicates(substitutedCondition), other, aliases)

      case other =>
        (None, Nil, other, Map.empty)
    }

  private def collectAliases(fields: Seq[Expression]): Map[Attribute, Expression] = fields.collect {
    case a @ Alias(child, _) => a.toAttribute -> child
  }.toMap

  private def substitute(aliases: Map[Attribute, Expression])(expr: Expression): Expression = {
    expr.transform {
      case a @ Alias(ref: AttributeReference, name) =>
        aliases.get(ref).map(Alias(_, name)(a.exprId, a.qualifiers)).getOrElse(a)

      case a: AttributeReference =>
        aliases.get(a).map(Alias(_, a.name)(a.exprId, a.qualifiers)).getOrElse(a)
    }
  }
}

/**
 * A pattern that finds joins with equality conditions that can be evaluated using equi-join.
 *
 * Null-safe equality will be transformed into equality as joining key (replace null with default
 * value).
 */
object ExtractEquiJoinKeys extends Logging with PredicateHelper {
  /** (joinType, leftKeys, rightKeys, condition, leftChild, rightChild) */
  type ReturnType =
    (JoinType, Seq[Expression], Seq[Expression], Option[Expression], LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) =>
      logDebug(s"Considering join on: $condition")
      // Find equi-join predicates that can be evaluated before the join, and thus can be used
      // as join keys.
      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
      val joinKeys = predicates.flatMap {
        case EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) => Some((l, r))
        case EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) => Some((r, l))
        // Replace null with default value for joining key, then those rows with null in it could
        // be joined together
        case EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
          Some((Coalesce(Seq(l, Literal.default(l.dataType))),
            Coalesce(Seq(r, Literal.default(r.dataType)))))
        case EqualNullSafe(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
          Some((Coalesce(Seq(r, Literal.default(r.dataType))),
            Coalesce(Seq(l, Literal.default(l.dataType)))))
        case other => None
      }
      val otherPredicates = predicates.filterNot {
        case EqualTo(l, r) =>
          canEvaluate(l, left) && canEvaluate(r, right) ||
            canEvaluate(l, right) && canEvaluate(r, left)
        case other => false
      }

      if (joinKeys.nonEmpty) {
        val (leftKeys, rightKeys) = joinKeys.unzip
        logDebug(s"leftKeys:$leftKeys | rightKeys:$rightKeys")
        Some((joinType, leftKeys, rightKeys, otherPredicates.reduceOption(And), left, right))
      } else {
        None
      }
    case _ => None
  }
}


/**
 * A pattern that finds joins with equality conditions that can be evaluated using range-join.
 */
object ExtractRangeJoinKeys extends Logging with PredicateHelper {
  /** (joinType, Seq(leftStartKeys, leftEndKeys, rightStartKeys, rightEndKeys),
    * leftChild, rightChild) */
  type ReturnType =
    (JoinType, Seq[Expression], LogicalPlan, LogicalPlan)

  /**
  * The overlapping heuristic between intervals (a,b) and (c,d)) is:
  * a < b && c < d && a < c && c < b */
  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case join @ Join(left, right, joinType, condition) =>
      logDebug(s"Considering join on: $condition")
      val predicates = condition.map(splitConjunctivePredicates).getOrElse(Nil)
      /* Look for expressions a < b and c < d where a,b and c,d belong to the same LogicalPlan
      **/
      val leftRangeKeys = predicates.flatMap(x => x match {
        case LessThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((l, r))
        case LessThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((l, r))
        case GreaterThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((r, l))
        case GreaterThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, left)) =>
          Some((r, l))
        case _ => None
      })
      val rightRangeKeys = predicates.flatMap(x => x match {
        case LessThanOrEqual(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((l, r))
        case LessThan(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((l, r))
        case GreaterThan(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((r, l))
        case GreaterThanOrEqual(l, r) if (canEvaluate(l, right) && canEvaluate(r, right)) =>
          Some((r, l))
        case _ => None
      })
      /* Look for expressions a < c, b < d such that a, b belong to one LogicalPlan and
       * c, d belong to the other.
       */
      val heterogeneousKeys = predicates.flatMap(x => x match {
        case LessThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((l, r))
        case LessThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((l, r))
        case GreaterThan(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((r, l))
        case GreaterThanOrEqual(l, r) if (canEvaluate(l, left) && canEvaluate(r, right)) ||
          (canEvaluate(l, right) && canEvaluate(r, left)) => Some((r, l))
        case _ => None
      })
      /* Now verify that the condition does indeed describe an overlapping. The criteria are:
       * 1) There is exactly one element on the left and right rangeKeys
       * (let's call them (a,b) and (c,d) respectively)
       * 2) There are exectly two elements on the heterogeneousKeys and they are of the form of
       * either ((a, c), (c, b)) or ((c, a), (a, d))
       **/

      (leftRangeKeys, rightRangeKeys, heterogeneousKeys) match{
        case (l, r, _) if (l.size != 1 || r.size != 1) => None
        case (_, _, h) if (h.size != 2) => None
        case (l, r, h) if ((h.contains((l.head._1, r.head._1)) //a-c, c-b
          && h.contains((r.head._1, l.head._2))) ||
          (h.contains((r.head._1, l.head._1)) && //c-a, a-d
            h.contains((l.head._1, r.head._2)))
          ) => Some((joinType,
          List(l.head._1, l.head._2, r.head._1, r.head._2).toSeq,
          left, right))
      }
    case _ => None
  }
}

/**
 * A pattern that collects all adjacent unions and returns their children as a Seq.
 */
object Unions {
  def unapply(plan: LogicalPlan): Option[Seq[LogicalPlan]] = plan match {
    case u: Union => Some(collectUnionChildren(u))
    case _ => None
  }

  private def collectUnionChildren(plan: LogicalPlan): Seq[LogicalPlan] = plan match {
    case Union(l, r) => collectUnionChildren(l) ++ collectUnionChildren(r)
    case other => other :: Nil
  }
}
