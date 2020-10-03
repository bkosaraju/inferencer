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
import org.apache.spark.sql.types.{ArrayType, DoubleType}

trait GetTargetSelListTests extends AppInterface {

  private val t2SQL = """select cast('2018-12-14 12:10:21' as timestamp) as src_timestamp, cast ('2018-04-15' as date) as src_date"""
  private val tDF = context.sql(t2SQL)
  private val nonPrimitveSchema=tDF.schema.add("nonPrimitiveColumn",ArrayType(ArrayType(ArrayType(DoubleType,true),true),true),true)

  test ("getTargetSelList : Generate the Schema driven SQL",appFunctionTests) {
    assertResult(Seq("cast(`src_timestamp` as timestamp)","cast(`src_date` as date)").mkString(",\n")) {
      af.getTargetSelList(tDF.schema)
    }
  }

  test ("getTargetSelList : Generate the Schema driven SQL with excluded non primitive types",appFunctionTests) {
    assertResult(Seq("cast(`src_timestamp` as timestamp)","cast(`src_date` as date)","`nonPrimitiveColumn`").mkString(",\n")) {
      af.getTargetSelList(nonPrimitveSchema)
    }
  }

  test ("getTargetSelList : Exception in case if not be able extract target selection list",appFunctionTests) {
    intercept[Exception] {
      af.getTargetSelList(null)
    }
  }



}
