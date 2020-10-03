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

import java.util.UUID.randomUUID

import io.github.bkosaraju.inferencer.appFunctionTests
import io.github.bkosaraju.inferencer.{AppInterface, appFunctionTests}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.StructType

trait GetTargetValidationFilter extends AppInterface {

  private val t2SQL = """select cast('somethext' as string) as src_text, cast (123 as int) as src_int"""
  private val tDF = context.sql(t2SQL)

  test ("getTargetValidationFilter : Generate the Schema driven SQL for Filter Condition ",appFunctionTests) {
//    assertResult(Seq("(src_text is null or (src_text is not null and  cast(src_text as string)=src_text))","(src_int is null or (src_int is not null and  cast(src_int as integer)=src_int))").mkString(" and \n")) {
      assertResult(Seq(" and (`src_int` is null or (`src_int` is not null and  cast(`src_int` as integer)=`src_int`))").mkString(" and \n")) {
      af.getTargetValidationFilter(tDF.schema)
    }
  }

  test ("getTargetValidationFilter : Exception in case if not be able extract target validation filter",appFunctionTests) {
    intercept[Exception] {
      af.getTargetValidationFilter(null)
    }
  }

  test ("getTargetValidationFilter : Empty schema",appFunctionTests) {
    assertResult("") {
      af.getTargetValidationFilter(new types.StructType())
    }
  }

}
