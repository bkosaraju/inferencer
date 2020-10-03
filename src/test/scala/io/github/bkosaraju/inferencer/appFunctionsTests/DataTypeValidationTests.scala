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

import java.util.Properties

import io.github.bkosaraju.inferencer.appFunctionTests
import io.github.bkosaraju.inferencer.{AppInterface, appFunctionTests}
import io.github.bkosaraju.inferencer.functions.Session
import org.apache.hadoop.fs.Path

trait DataTypeValidationTests extends AppInterface with Session {
  private val location = new Path("src/test/resources/appResources/textAppDataTypeCheckExit.properties")
  private val hadoopfs = location.getFileSystem(context.sessionState.newHadoopConf())
  private val cp = lp.loadCustomProperties(hadoopfs, "src/test/resources/appResources/textAppDataTypeCheckExit.properties", new Properties())

  private val tgtSchema = af.loadSchema(cp.getProperty("targetDatabase"), cp.getProperty("targetTable"))
  private val dwsVals = af.stringToMap(cp.getProperty("dwsVars", ""))
  private val dwsCols = dwsVals.keys.toSeq
  private val srcSchema = af.getStgSchema(tgtSchema, dwsCols)(0)
  private val srttoTgtMap = af.stringToMap(cp.getProperty("srcToTgtColMap", ""))
  private val srcConvSchema = af.getStgSchema(tgtSchema, dwsCols, srttoTgtMap)(1)
  private val sRDD = af.loadDataFile("src/test/resources/csvdatasets/rand_data_invalidDataTypes.csv")
  private val srcDF = context.createDataFrame(sRDD, srcSchema)
  private val srcFormats = af.stringToMap(cp.getProperty("srcFormats", "").replaceAll("\"", ""))
  private val convSrcDF = af.convNonStdDateTimes(srcDF, srcFormats, tgtSchema,cp.getProperty("errorThresholdPercent","100").replaceAll("%",""))



  test("dataTypeValidationTests : Able to throw the exception in case of reaching the threshold data quality >4% invalid records ", appFunctionTests) {
    intercept[ErrorRecordThresholdException] {
      val df = af.dataTypeValidation(convSrcDF, srcDF, srcConvSchema, cp)
      val cnt = df(0).count()
    }
  }
}
