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
import org.apache.spark.sql.functions.col

trait WriteHiveTableTests extends AppInterface {
  private val df = context.read.format("orc").load("src/test/resources/orcdatasets/randdata.orc")

  private val orcTbl = "a"+randomUUID().toString.substring(0,4)
  private val orcTblwP = "a"+randomUUID().toString.substring(0,4)

  context.sql("CREATE  TABLE if not exists "+orcTbl+"(src_timestamp string, src_date string, Keycol string, ValueCol string) STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
  context.sql("CREATE  TABLE if not exists "+orcTblwP+"(src_timestamp string, Keycol string, ValueCol string) PARTITIONED BY (src_date string) STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  test("writeHiveTable : Test to write Data Into Hive (overwrite)", appFunctionTests) {

    assertResult(true) {
      af.writeHiveTable(df, "default", orcTbl, "overwrite")
      true
    }
  }
  test("writeHiveTable : Test to write Data Into Hive (append) ", appFunctionTests) {
    assertResult(true) {
      af.writeHiveTable(df, "default", orcTbl, "append")
      true
    }
  }

  test("writeHiveTable : Test to write Data Into Hive - partition - append ", appFunctionTests) {
    assertResult(true) {
      af.writeHiveTable(df.filter(col("src_date").equalTo("7/10/2018"))
        , "default", orcTblwP, "append", "src_date")
      true
    }
  }
  test("writeHiveTable : Test to write Data Into Hive - partition - overwrite", appFunctionTests) {
    assertResult(true) {
      af.writeHiveTable(df.filter(col("src_date").equalTo("7/10/2018"))
        , "default", orcTblwP, "overwrite", "src_date")
      true
    }
  }

  test("writeHiveTable : Unable to write to the table and throws exception in case if table does not exists", appFunctionTests) {
    intercept[Exception] {
      af.writeHiveTable(df, "default", "nonExistedTable", "append", "src_date")
    }
  }
}
