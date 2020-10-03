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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.Mockito
import org.mockito.Mockito.when

trait GetStgSchemaTests extends AppInterface {

  private val t1SQL = """select '2018/12/14 12:10:21' as src_timestamp, '15/04/2018' as src_date"""
  private val sDF = context.sql(t1SQL)

  test ("getStgSchema : Derive the source schema from curated schema - check for non mapped column",appFunctionTests) {
    assertResult(
      StructType(Seq(StructField("src_timestamp",StringType,false)))
    ) {
      af.getStgSchema(sDF.schema,Seq("src_date"))(0)
    }
  }

  test ("getStgSchema : Derive the source schema from curated schema - check for mapped column",appFunctionTests) {
    assertResult(
      StructType(Seq(StructField("src_timestamp_column",StringType,false),StructField("src_date",StringType,false)))
    ) {
      af.getStgSchema(sDF.schema,Seq("src_date"),Map("src_timestamp_column" -> "src_timestamp"))(1)
    }
  }

  private val srcSchema = StructType(Seq(
    StructField("src_timestamp", StringType , true),
    StructField("src_date", StringType, true),
    StructField("Keycol", StringType, true),
    StructField("ValueCol", StringType, true)))
  private val m = Seq("a","b")
  private val s = Mockito.spy(srcSchema)
  when(s.iterator).thenThrow(new RuntimeException("Explicit Error Thrown.."))
  test("getStgSchema : Unable to generate staging schema and raise exception in case if there is any issue with source schema", appFunctionTests) {
    intercept[Exception] {
      af.getStgSchema(s,m)
    }
  }


}
