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
import java.util.UUID.randomUUID

import io.github.bkosaraju.inferencer.appFunctionTests
import io.github.bkosaraju.inferencer.{AppInterface, appFunctionTests}
import org.apache.hadoop.fs.Path

trait LoadCuratedDataTests extends AppInterface {

  private val location = new Path("src/test/resources/appResources/textApp.properties")
  private val hadoopfs = location.getFileSystem(context.sessionState.newHadoopConf())
  private val prop = new Properties()


  context.sql("Drop table if exists tgtBinaryTbl")
  context.sql("CREATE TABLE tgtBinaryTbl(src_timestamp string, src_date string, Keycol string, ValueCol string) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
  private val customPropsBinary = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/textApp.properties",prop)
  test ("loadCuratedTable : load the curated Table from Binary File ",appFunctionTests) {
    assertResult(true) {
      lc.loadCuratedData(customPropsBinary)
      true
    }
  }

  private val jsonProps = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/jsonApp.properties",prop)
  test ("loadCuratedTable : load the curated Table from Json File ",appFunctionTests) {
    assertResult(true) {
      lc.loadCuratedData(jsonProps)
      true
    }
  }
  private val tp = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/textAppDataTypeCheck.properties",prop)
  test ("loadCuratedTable : load the curated Table under the throshold record counts ",appFunctionTests) {
    assertResult(true) {
      lc.loadCuratedData(tp)
      true
    }
  }

  private val jp = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/jsonApp.properties",new Properties())

  test ("loadCuratedTable : load the json file to a dataframe ",appFunctionTests) {
    assertResult(true) {
    lc.loadCuratedData(jp)
      true
    }
  }


  context.sql("Drop Table If exists tgtCsvTblWithTags")
  context.sql("CREATE TABLE tgtCsvTblWithTags(src_timestamp string, src_date string, Keycol string, ValueCol string,tag_src_date integer, tag_src_hour integer) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  private val taggedProps = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/csvApp_withTaggedCols.properties",new Properties())

  test ("loadCuratedTable : load the CSV with tagged columns to a dataframe ",appFunctionTests) {
    assertResult(500) {
      lc.loadCuratedData(taggedProps)
      val cDF = context.table("default.tgtCsvTblWithTags")
      cDF.show(20,false)
      cDF.count()
    }
  }

  context.sql("Drop Table If exists tgtCsvTblWithTagsandRunTimeVars")
  context.sql("CREATE TABLE tgtCsvTblWithTagsandRunTimeVars(src_timestamp string, src_date string, Keycol string, ValueCol string,tag_src_date date, tag_src_hour integer) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  private val taggedwithRunTimeProps = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/csvApp_withTaggedAndRuntimeCols.properties",new Properties())

  test ("loadCuratedTable : load the CSV with tagged columns to a dataframe with Runtime columns ",appFunctionTests) {
    assertResult(500) {
      lc.loadCuratedData(taggedwithRunTimeProps)
      val cDF = context.table("default.tgtCsvTblWithTagsandRunTimeVars")
      cDF.show(20,false)
      cDF.count()
    }
  }

  context.sql("Drop Table If exists tgtCsvTblWithTagsandRunTimeVars_2")
  context.sql("CREATE TABLE tgtCsvTblWithTagsandRunTimeVars_2(src_timestamp string, src_date string, Keycol string, ValueCol string,tag_src_date date, tag_src_hour integer) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  private val taggedwithRunTimeProps_2 = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/csvApp_withTaggedAndRuntimeCols_2.properties",new Properties())

  test ("loadCuratedTable : load the CSV with tagged columns to a dataframe with Runtime columns with date Format YYYYDDMM Format ",appFunctionTests) {
    assertResult(500) {
      lc.loadCuratedData(taggedwithRunTimeProps_2)
      val cDF = context.table("default.tgtCsvTblWithTagsandRunTimeVars_2")
      cDF.show(20,false)
      cDF.count()
    }
  }

  context.sql("drop table if exists tgtBinarValidDataTypeCurateApp")
  private val curateTableLocation="build/tmp/hive/"+randomUUID().toString
  context.sql("CREATE  TABLE tgtBinarValidDataTypeCurateApp(src_timestamp timestamp, src_date date, Keycol int, ValueCol string) " +
    "STORED AS ORC LOCATION '"+curateTableLocation+"'")
  private val curateAppProps = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/textAppDataTypeCheckCurateApp.properties",new Properties())
  curateAppProps.put("targetURI","build/tmp/errorrecords")
  test("loadCuratedTable : load the data into target bucket in case if error threshold is under specified limit ", appFunctionTests) {
    assertResult(472) {
    lc.loadCuratedData(curateAppProps)
      context.table("tgtBinarValidDataTypeCurateApp").count()
    }
  }

  test("loadCuratedTable : load the data into error bucket in case if error threshold is under specified limit for faulty records", appFunctionTests) {
    assertResult(29) {
      val errDF = context.read.orc("build/tmp/errorrecords/csvdatasets.rejected")
      errDF.show(100,false)
      errDF.count()
    }
  }
}
