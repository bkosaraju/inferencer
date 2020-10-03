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

trait LoadTableandRegisterTests extends AppInterface {
  context.sql ("drop table if exists default.tableReaderCheck")
  context.sql("CREATE TABLE if not exists default.tableReaderCheck(src_timestamp string, src_date string, Keycol string, ValueCol string) STORED AS ORC")

  test("loadTableandRegister : Load Hive table to a dataframe ",appFunctionTests) {
    assertResult(0) {
      af.loadTableandRegister("default","tableReaderCheck","SomeTempView").count
    }
  }

  test("loadTableandRegister : Load Hive table to a dataframe - with Temp View ",appFunctionTests) {
    af.loadTableandRegister("default","tableReaderCheck","SomeTempView")
    assertResult(0) {
      context.sql("select * from SomeTempView").count()
    }
  }


  test("loadTableandRegister : Unable to load Table and throws exception for given table if a table and database not existed", appFunctionTests) {
    intercept[Exception] {
      af.loadTableandRegister("someDB","SomeUnKnowTable","someTemTableName")
    }
  }


  test("loadTableandRegister : Unable to load table and throws exception for given table if a table not existed", appFunctionTests) {
    intercept[Exception] {
      af.loadTableandRegister("default","SomeUnKnowTable")
    }
  }


  test("loadTableandRegister : Unable to load table and throws exception for given table if a database not existed", appFunctionTests) {
    intercept[Exception] {
      af.loadTableandRegister("someUnknownDB","tableReaderCheck")
    }
  }


}
