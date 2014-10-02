/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import org.apache.spark.sql.{SQLContext, QueryTest}
import org.apache.spark.sql.test._

case class RecordData1(start1: Long, end1: Long) extends Serializable
case class RecordData2(start2: Long, end2: Long) extends Serializable
case class GenomicRegion1(start1: Long, end1: Long, chr1: String) extends Serializable
case class GenomicRegion2(start2: Long, end2: Long, chr2: String) extends Serializable

class SQLRangeJoinSuite extends QueryTest {
  val sc = TestSQLContext.sparkContext
  val sqlContext = new SQLContext(sc)
  import sqlContext._

  test("joining non overlappings results into no entries") {
    val rdd1 = sc.parallelize(Seq((1L, 5L), (2L, 7L))).map(i => RecordData1(i._1, i._2))
    val rdd2 = sc.parallelize(Seq((11L, 44L), (23L, 45L))).map(i => RecordData2(i._1, i._2))
    rdd1.registerTempTable("t1")
    rdd2.registerTempTable("t2")
    checkAnswer(
      sql("select * from t1 RANGEJOIN t2 on OVERLAPS( (start1, end1), (start2, end2))"),
      /*sql("select * from t1 JOIN t2 on " +
        "start1 < end1 and start2 < end2 and start1 < start2 and start2 < end1"),*/
      Nil
    )
  }

  test("basic range join") {
    val rdd1 = sc.parallelize(Seq((100L, 199L),
      (200L, 299L),
      (400L, 600L),
      (10000L, 20000L)))
      .map(i => RecordData1(i._1, i._2))
    val rdd2 = sc.parallelize(Seq((150L, 250L),
      (300L, 500L),
      (500L, 700L),
      (22000L, 22300L)))
      .map(i => RecordData2(i._1, i._2))
    rdd1.registerTempTable("s1")
    rdd2.registerTempTable("s2")
    checkAnswer(
      sql("select start1, end1, start2, end2 from s1 RANGEJOIN s2 on OVERLAPS( (start1, end1), (start2, end2))"),
      /*sql("select * from s1 JOIN s2 on " +
        "start1 < end1 and start2 < end2 and start1 < start2 and start2 < end1"),*/
      (100L, 199L, 150L, 250L) ::
        (200L, 299L, 150L, 250L) ::
        (400L, 600L, 300L, 500L) ::
        (400L, 600L, 500L, 700L) :: Nil
    )
    checkAnswer(
      sql("select end1 from s1 RANGEJOIN s2 on OVERLAPS( (start1, end1), (start2, end2))"),
     /* sql("select end1 from s1 JOIN s2 on " +
        "start1 < end1 and start2 < end2 and start1 < start2 and start2 < end1"),*/
      Seq(199L) :: Seq(299L) :: Seq(600L) :: Seq(600L) :: Nil
    )
  }

  test("range join with genomic overlaps"){
    val rdd1 = sc.parallelize(Seq((100L, 199L, "chr1"),
      (200L, 299L, "chr1"),
      (400L, 600L, "chr1"),
      (1000L, 2000L, "chr2"),
      (100L, 199L, "chr13"),
      (200L, 299L, "chr13"),
      (400L, 600L, "chr13")))
    .map(i => GenomicRegion1(i._1, i._2, i._3))
    val rdd2 = sc.parallelize(Seq((150L, 250L, "chr1"),
      (150L, 250L, "chr13"),
      (1400L, 1600L, "chr1"),
      (150L, 250L, "chr2"),
      (150L, 250L, "chr3"),
      (150L, 250L, "chr4"),
      (150L, 250L, "chr5"),
      (150L, 250L, "chr15")))
    .map(i => GenomicRegion2(i._1, i._2, i._3))
    rdd1.registerTempTable("s1")
    rdd2.registerTempTable("s2")

    checkAnswer(
    sql("select start1, end1, chr1, start2, end2, chr2 from s1 RANGEJOIN s2 on " +
      "GENOMEOVERLAP( (start1, end1, chr1), (start2, end2, chr2) )"),
      (100, 199, "chr1", 150, 250, "chr1") ::
        (200, 299, "chr1", 150, 250, "chr1") ::
        (100, 199, "chr13", 150, 250, "chr13") ::
        (200, 299, "chr13", 150, 250, "chr13") :: Nil
    )

  }
}
