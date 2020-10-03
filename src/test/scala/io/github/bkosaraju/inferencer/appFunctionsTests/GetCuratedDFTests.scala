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

import io.github.bkosaraju.inferencer.AppFunctions
import io.github.bkosaraju.inferencer.{AppFunctions, AppInterface, appFunctionTests}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.mockito.Matchers.any
import org.mockito.Mockito
import org.mockito.Mockito.doThrow

trait GetCuratedDFTests extends AppInterface {

  private val srcSchema = StructType(Seq(
    StructField("src_timestamp", StringType , true),
    StructField("src_date", StringType, true),
    StructField("Keycol", StringType, true),
    StructField("ValueCol", StringType, true))
  )
  private val location = new Path("src/test/resources/appResources/csvApp.properties")
  private val hadoopfs = location.getFileSystem(context.sessionState.newHadoopConf())


  context.sql("Drop Table If exists tgtCsvTbl")
  context.sql("Drop Table If exists tgtBinaryTbl")
  context.sql("Drop Table If exists tgtBinaryTblColMap")
  context.sql("Drop Table If exists siiamBinary")
  context.sql("Drop Table If exists tgtBinarValidDataType")
  context.sql("Drop Table If exists tgtBinarValidDataTypeException")
  context.sql("Drop Table If exists tgtDataTypeException")
  context.sql("Drop Table If exists device_bdcx")


  context.sql(
    """
      | CREATE TABLE `siiamBinary`(
      |   `basic_access_pcms_code` string,
      |   `bus_unit` string,
      |   `connection_date` date,
      |   `csg_count` bigint,
      |   `csg_excluded_product` bigint,
      |   `customer_id` string,
      |   `disconnection_date` date,
      |   `div_to_srv_num` string,
      |   `ebill_pstn` bigint,
      |   `exchange_code` string,
      |   `is_prassist` bigint,
      |   `is_redirection_prd` bigint,
      |   `national_number` string,
      |   `nbn_sio` string,
      |   `pa_excluded_product` bigint,
      |   `prassist_count` bigint,
      |   `prassist_reg_date` date,
      |   `prd_effective_date` date,
      |   `service_status` string,
      |   `sio_is_csg_elig` bigint,
      |   `sio_is_ngf` bigint)
    """.stripMargin+"STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
  context.sql("CREATE TABLE tgtCsvTbl(src_timestamp string, src_date string, Keycol string, ValueCol string) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  context.sql("CREATE  TABLE tgtBinaryTbl(src_timestamp string, src_date string, Keycol string, ValueCol string) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
  context.sql("CREATE  TABLE tgtBinaryTblColMap(src_timestamp_view string, src_date string, Keycol string, ValueCol string) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
  context.sql("CREATE  TABLE tgtBinarValidDataType(src_timestamp timestamp, src_date date, Keycol int, ValueCol string) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
  context.sql("CREATE  TABLE tgtBinarValidDataTypeException(src_timestamp timestamp, src_date date, Keycol int, ValueCol string) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  context.sql("CREATE  TABLE device_bdcx(collection_date string, device_deviceinfo_memorystatus_total string ) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")


  context.sql("CREATE  TABLE tgtDataTypeException(src_timestamp timestamp, src_date date, Keycol int, ValueCol string) " +
    "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  private val customPropsCsv = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/csvApp.properties",new Properties())

  test ("getCuratedDF : load the CSV file to a dataframe",appFunctionTests) {
    assertResult(500) {
      af.getCuratedDF("src/test/resources/csvdatasets/rand_data.csv",customPropsCsv)(0).count()
    }
  }

  private val customPropsBinary = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/textApp.properties",new Properties())

  test ("getCuratedDF : load the binary file to a dataframe",appFunctionTests) {
    assertResult(501) {
      af.getCuratedDF("src/test/resources/csvdatasets/rand_data.csv",customPropsBinary)(0).count()
    }
  }

  private val customPropsBinarydataTypeCheck = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/textAppDataTypeCheck.properties",new Properties())

  test ("getCuratedDF : load the binary file to a dataframe - >5% invalid dataTypes",appFunctionTests) {
     val df = af.getCuratedDF("src/test/resources/csvdatasets/rand_data_invalidDataTypes.csv",customPropsBinarydataTypeCheck)
    assertResult(472) {
      df(0).show(500)
      df(0).count()
    }
  }

  private val jp = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/jsonApp.properties",new Properties())

  test ("getCuratedDF : load the json file to a dataframe ",appFunctionTests) {
    assertResult(2) {
    val df = af.getCuratedDF("src/test/resources/jsondatasets/jsonDataForWrapperTest.json",jp)
      df(0).count()
    }
  }

  private val jpdttm = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/jsonAppcolRename.properties",new Properties())

  test ("getCuratedDF : load the json file to a dataframe with target column rename in congention with format change ",appFunctionTests) {
    assertResult(2) {
      val df = af.getCuratedDF("src/test/resources/jsondatasets/josnDateTimeColMap.json",jpdttm)
      df(0).show(false)
      df(0).count()
    }
  }

  private val customPropsSiiam = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/siiam.properties",new Properties())

  test ("getCuratedDF : load the siiam binary file to a dataframe",appFunctionTests) {
    val siiamDF = af.getCuratedDF("src/test/resources/binarydatasets/siiam.txt",customPropsSiiam)(0)
    siiamDF.printSchema()
    siiamDF.show(10,false)
    assertResult(6) {
      siiamDF.count()
    }
  }

  private val appfunctions = new AppFunctions
  private val mk = Mockito.spy(appfunctions)
  doThrow(new RuntimeException("Explicit Error.."))
    .when(mk)
    .amendDwsCols(any(classOf[DataFrame]),any(classOf[Map[String,String]]),any(classOf[StructType]))

  test("getCuratedDF : Unable curate the table and throws exception in case if there is any with application functions", appFunctionTests) {
    intercept[Exception] {
      mk.getCuratedDF("src/test/resources/csvdatasets/rand_data.csv",customPropsCsv)(0).count()
    }
  }


  private val jpwithinferTargetSchema = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/jsonwithinferTargetSchema.properties",new Properties())

  test ("getCuratedDF : load the json file with infer schema to a dataframe ",appFunctionTests) {
    assertResult(2) {
      val df = af.getCuratedDF("src/test/resources/jsondatasets/jsonWithinferTargetSchema.json",jpwithinferTargetSchema)
      df(0).show(false)
      df(0).count()
    }
  }




}
