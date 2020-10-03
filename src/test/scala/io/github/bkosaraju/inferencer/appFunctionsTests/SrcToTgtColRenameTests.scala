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

trait SrcToTgtColRenameTests extends AppInterface {

  private val srcSchema = StructType(Seq(
    StructField("src_timestamp", StringType , true),
    StructField("src_date", StringType, true),
    StructField("Keycol", StringType, true),
    StructField("ValueCol", StringType, true))
  )
  private val srcDf = af.loadStdDF("src/test/resources/csvdatasets/rand_data.csv","csv",Map("header" -> "true"),srcSchema)

  test ("srcToTgtColRename : rename the source columns to target columns",appFunctionTests) {
    val targetSchema = StructType(
      srcSchema.flatMap( x => if (x.name == "ValueCol") Some(x.copy("ValueColumn",StringType,true)) else Some(x))
    )
    assertResult(targetSchema) {
      af.srcToTgtColRename(srcDf,Map("ValueCol" -> "ValueColumn","SomeUnknown"->"SomeValueColumn","K"->"V")).schema
    }
  }

  val m = Mockito.spy(Map[String,String]("a"->"b"))
  when(m.keys).thenThrow(new RuntimeException("Explicit Error Thrown.."))


  test("srcToTgtColRename : Unable to rename column and throws exception in case if there is any issue with given input map", appFunctionTests) {
    intercept[Exception] {
      af.srcToTgtColRename(srcDf, m).schema
    }
  }
}
