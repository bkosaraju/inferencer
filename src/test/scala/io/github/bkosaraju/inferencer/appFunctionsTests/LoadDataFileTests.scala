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
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.mockito.Matchers.{any, isA}

trait LoadDataFileTests extends AppInterface {

  test ("loadDataFile : load data file into row of RDD - no clence ",appFunctionTests) {
    val resRDD = af.loadDataFile("src/test/resources/csvdatasets/rand_data.csv","\n",",","","text")
    assertResult(501){
      resRDD.count()
    }
  }
  test ("loadDataFile : load data file into row of RDD - with clensing ",appFunctionTests) {
    val resRDD = af.loadDataFile("src/test/resources/csvdatasets/rand_data.csv","\n",",","e53dabba-227d-4b4a-8a07-bf415731cfdf","text")
    assertResult(0){
      resRDD.map(x => Option(x(3)) match  {
        case  Some("e53dabba-227d-4b4a-8a07-bf415731cfdf") => true
        case _ => false }).filter(x => (x))
      .count()
    }
  }

  test ("loadDataFile : load data file into row of RDD - read the file in binary format ",appFunctionTests) {
    val resRDD = af.loadDataFile("src/test/resources/csvdatasets/rand_data.csv", "\n", ",", "", "binary")
    assertResult(501) {
      resRDD.count()
    }
  }

  test ("loadDataFile : load data file into row of RDD - read the file in binary format - default options ",appFunctionTests) {
    val resRDD = af.loadDataFile("src/test/resources/csvdatasets/rand_data.csv")
    assertResult(501) {
      resRDD.count()
    }
  }

//  private  val mloadDataFile = af
//  private  val mloadDataFileSpy = Mockito.spy(mloadDataFile)
//  when(mloadDataFileSpy.loadRdd(isA(classOf[String]),isA(classOf[String]))).thenThrow(new RuntimeException("Explicit Error Thrown.."))
  test ("loadDataFile : load data file into row of RDD - Raise an exception in case if there is any issue with loading the file ",appFunctionTests) {
    intercept[Exception] {
      val resRDD = af.loadDataFile("","\n",",")
      resRDD.count()
    }
  }
}
