/*
 *  Copyright (C) 2019-2020 bkosaraju
 *  All Rights Reserved.
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.github.bkosaraju.inferencer.appFunctionsTests

import io.github.bkosaraju.inferencer.appFunctionTests
import io.github.bkosaraju.inferencer.{AppInterface, appFunctionTests}
import org.apache.spark.sql.functions.lit

trait IsSchemaSameTests extends AppInterface {

  private val t2SQL = """select cast('2018-12-14 12:10:21' as timestamp) as src_timestamp, cast ('2018-04-15' as date) as src_date"""
  private val tDF = context.sql(t2SQL)
  private val t1SQL = """select '2018/12/14 12:10:21' as src_timestamp, '15/04/2018' as src_date"""
  private val sDF = context.sql(t1SQL)

  test("isSchemaSame : Schema Comparision Utility - equal context",appFunctionTests) {
    assert {af.isSchemaSame(tDF.schema, tDF.schema) }
  }

  test("isSchemaSame : Schema Comparision Utility - not equal context",appFunctionTests) {
    assertResult(false) { af.isSchemaSame(sDF.schema,sDF.drop("src_date").schema)}
  }

  test("isSchemaSame : Schema Comparision Utility - in order items ",appFunctionTests) {
    assert {
      af.isSchemaSame(
        sDF.schema,
        sDF.drop("src_timestamp").withColumn("src_timestamp",lit("2018-06-18 10:05:05")).schema
      )}
  }

  test ("isSchemaSame : select only subset of total columns - sortAndConv",appFunctionTests) {
    assert {
      af.isSchemaSame(tDF.drop("src_timestamp").schema,tDF.schema)
    }
  }
//
//  test("isSchemaSame : Schema Comparision Utility - test for sortAndConv ",appFunctionTests) {
//    assertResult(Array("src_timestamp, src_date").sortWith(_>_)) {
//      val sortAndConv = af.isSchemaSame(sDF.schema,sDF.schema)
//        .getClass.getMethod("sortAndConv")
//      sortAndConv(sDF.schema)
//      val nis = new af.isSchemaSa
//      }
//  }

}
