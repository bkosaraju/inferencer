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

import io.github.bkosaraju.inferencer.AppFunctions
import io.github.bkosaraju.inferencer.{AppFunctions, AppInterface, appFunctionTests}
import org.apache.hadoop.fs.Path
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.mockito.Mockito.doThrow

trait RemoveHdfsDataTests extends AppInterface {

  test ("removeHdfsData : remove hdfs path",appFunctionTests) {
    assertResult(true) {
      af.removeHdfsData("build/tmp/testlocation")
      true
    }
  }
test ("removeHdfsData : remove hdfs path - not to delete root path accidentally ",appFunctionTests) {
    assertResult(true) {
      af.removeHdfsData("/")
      true
    }
  }

  //Create the file before removal for test

  val location = new Path("build/tmp/testLocation")
  val hadoopfs = location.getFileSystem(context.sessionState.newHadoopConf())
  hadoopfs.create(location,true)

  test ("removeHdfsData : remove hdfs path - delete the tableURI ",appFunctionTests) {
    assertResult(true) {
      af.removeHdfsData("build/tmp/testLocation")
      true
    }
  }


  private val appfunctions = new AppFunctions
  private val mk = Mockito.spy(appfunctions)
  doThrow(new RuntimeException("Explicit Error.."))
    .when(mk)
    .removeHdfsData(any(classOf[String]))

  test("removeHdfsData : Unable remove specified path(non existed) and throws exception in case if there is any with application functions", appFunctionTests) {
    intercept[Exception] {
      mk.removeHdfsData("someLocation")
    }
  }


  test("removeHdfsData : Unable remove specified path(exception test) and throws exception in case if there is any with application functions", appFunctionTests) {
    intercept[Exception] {
      mk.removeHdfsData(null)
    }
  }
}
