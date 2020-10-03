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

trait StringToMapTests extends AppInterface {
  test("stringToMap : Test to be able to convert input params String to Map ",appFunctionTests) {
    assertResult("Value2") {
      af.stringToMap("Key1=Value1,Key2=Value2")("Key2")
    }
  }

  test("stringToMap : Unable to convert given string into a Map as that was not in valid keyvalue pair format(k1=v1)", appFunctionTests) {
    intercept[Exception] {
      af.stringToMap("testString Not In Key value Format")    }
  }

}
