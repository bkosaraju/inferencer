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
import io.github.bkosaraju.inferencer.functions.Session

trait GetLatestDataSetsTest extends AppInterface with Session {
  private val location = "src/test/resources/sampleDataSets"

  test ("getLatestDataSetsTest : Validating the result for given overwrite Mode", appFunctionTests) {
    assert {
      af.getLatestDataSets(location,"text","overwrite").mkString.contains("rand_data_20180924_101524.csv") &&
        ! af.getLatestDataSets(location,"text","overwrite").mkString.contains("abc_20180828.txt")
    }
  }

for (fileFormat <- List("orc", "parquet", "avro", "com.databricks.spark.avro")) {
  for (writeMode <- List("append","overWrite")) {

    test ("getLatestDataSetsTest : Validating the result for given overwrite Mode with " + fileFormat +" and writeMode "+ writeMode , appFunctionTests) {
      assertResult(location) {
        af.getLatestDataSets(location,"orc","overwrite").mkString
      }
    }
  }
}

  test("getLatestDataSetsTest : Able to throw the exception in case of source Directory not existed ", appFunctionTests) {
    intercept[Exception] {
      af.getLatestDataSets("/testDataset/NotKnownLocation","text","overWrite")
    }
  }
}
