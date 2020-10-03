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

import java.io.{BufferedOutputStream, FileNotFoundException}
import java.util.Properties

import io.github.bkosaraju.inferencer.projectSpecificTests
import io.github.bkosaraju.inferencer.{AppInterface, projectSpecificTests}
import org.apache.hadoop.fs.Path

trait LoadPropertiesTests extends AppInterface {

  test ("loadLocalappProps : Test to be able to load the local properties",projectSpecificTests) {
    assertResult("") {
      lp.loadLocalappProps().getProperty("basePath","")
    }
  }

  test ("loadLocalappProps : Test to be able to load the non Existed local properties",projectSpecificTests) {
    assertResult(null) {
      lp.loadLocalappProps("nonExistedProps").getProperty("basePath")
    }
  }

  private val location = new Path("build/tmp/properties/testProps")
  private val hadoopfs = location.getFileSystem(context.sessionState.newHadoopConf())
  private val op = hadoopfs.create(location,true)
  private val OutputStream = new BufferedOutputStream(op)
  OutputStream.write("appName=TestApplication".getBytes("UTF-8"))
  OutputStream.close()

  test ("loadCustomProperties : remove hdfs Properties ",projectSpecificTests) {
    assertResult("TestApplication") {
      lp.loadCustomProperties(hadoopfs,"build/tmp/properties/testProps",new Properties()).getProperty("appName")
    }
  }

  test ("loadCustomProperties : remove hdfs Properties - unknown path",projectSpecificTests) {
    assertThrows[FileNotFoundException]{
      lp.loadCustomProperties(hadoopfs,"/non/existed",new Properties()).getProperty("appName")
    }
  }

  test ("loadCustomProperties : remove hdfs Properties - Empty path",projectSpecificTests) {
    assertResult(null){
      lp.loadCustomProperties(hadoopfs,"",new Properties()).getProperty("appName")
    }
  }

}
