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

trait ExtractTaggedColumnsTests extends AppInterface {

  private val fileName = "abc_20180829_114526.txt"
  private val filePattern = """abc_(\d{8})_(\d{6}).txt"""
  private val nonMatchedPattern = """xxxxxx"""
  private val tokens = Array("src_date", "src_time")

  test("extractTaggedColumns : Extract the tokens out of the file Name ", appFunctionTests) {
    val Result: Map[String, String] = Map("src_date" -> "20180829", "src_time" -> "114526")
    assertResult("114526") {
      af.extractTaggedColumns(fileName, filePattern, tokens)("src_time")
    }
  }
  test("extractTaggedColumns : Exception in case of not been able to extract in case of token matching is not possible ", appFunctionTests) {
    intercept[Exception] {
      af.extractTaggedColumns(fileName, nonMatchedPattern, tokens)
    }
  }

  private val tokensTest = Array("src_date","src_time","Some_other_col_not_found")
  test("extractTaggedColumns : Exception in case of not enough tokens matched for the given senario ", appFunctionTests) {
    intercept[Exception] {
      af.extractTaggedColumns(fileName, filePattern, tokensTest)
    }
  }
  private val tokensTestFew = Array("src_date")
  test("extractTaggedColumns : log a warinig and continue in case few tokens requested than the selecetd ", appFunctionTests) {
    assertResult("20180829") {
      af.extractTaggedColumns(fileName, filePattern, tokensTestFew)("src_date")
    }
  }


}
