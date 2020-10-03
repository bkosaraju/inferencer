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

import java.util.InputMismatchException
import java.util.UUID.randomUUID

import io.github.bkosaraju.inferencer.{AppInterface, Inferencer, appFunctionTests}
import io.github.bkosaraju.inferencer.{AppInterface, Inferencer, appFunctionTests}
import org.apache.spark.sql.functions.{col, lit}

trait DriverTests extends AppInterface {
  test("driver : Able to run End to End Application", appFunctionTests) {
    context.sql("drop table If exists tgtBinaryTbl")
    context.sql("CREATE TABLE tgtBinaryTbl(src_timestamp string, src_date string, Keycol string, ValueCol string) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
    assertResult(true) {
      Inferencer.main(Array("src/test/resources/appResources/textApp.properties", "", "rand_data.csv"))
      true
    }
  }

  test("driver : Able to run End to End Application with spark application extra columns", appFunctionTests) {
    context.sql("drop table If exists tgtBinaryTbl")
    context.sql("CREATE TABLE tgtBinaryTbl(src_timestamp string, src_date string, Keycol string, ValueCol string,add_col String) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
    assertResult(true) {
      Inferencer.main(Array("src/test/resources/appResources/textApp.properties", "add_col=somedefaultedValue", "rand_data.csv"))
      true
    }
  }

  test("driver : Able to run End to End Application with spark application extra columns defaulted to NULL", appFunctionTests) {
    context.sql("drop table If exists tgtBinaryTbl")
    context.sql("CREATE TABLE tgtBinaryTbl(src_timestamp string, src_date string, Keycol string, ValueCol string,add_col String) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")
    assertResult(true) {
      Inferencer.main(Array("src/test/resources/appResources/textApp.properties", "add_col=null", "rand_data.csv"))
      true
    }
  }

  test ("driver : CSV file with no source formats specified while reading non standard dates should use default Spark - CSV parser behaviour") {
    context.sql("drop table If exists csvrandTbl")
    context.sql("CREATE TABLE csvrandTbl(src_timestamp timestamp, src_date date, Keycol int, ValueCol string) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    intercept[ErrorRecordThresholdException] {
    Inferencer.main(Array("src/test/resources/appResources/csvAppWithOutSrcFormats.properties", "", "rand_data.csv"))
    context.table("csvrandTbl").count()
  }
  }
  test("driver : Unable to commence execution and throws exception incase if given input parameters are not valid(too few)", appFunctionTests) {
    intercept[InputMismatchException] {
      Inferencer.main(Array())
    }
  }


  test ("driver : Quoted Data with pipe separation") {
    context.sql("drop table If exists quoted_data_tbl")
    context.sql("CREATE TABLE quoted_data_tbl(src_timestamp timestamp, src_date date, Keycol int, ValueCol string) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    assertResult(4) {
      Inferencer.main(Array("src/test/resources/appResources/quotedData.properties", "", "quoted_pipe.dat"))
      val tDF = context.table("quoted_data_tbl")
      tDF.show(false)
      tDF.count()
    }
  }

  test("driver : load the default data - not override the current data incase if data is present in source") {
    context.sql("drop table If exists quoted_data_tbl_defaultedData")
    context.sql("CREATE TABLE quoted_data_tbl_defaultedData(src_timestamp timestamp, src_date date, Keycol int, ValueCol string) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    assertResult(0) {
      Inferencer.main(Array("src/test/resources/appResources/quotedDatadefaultedData.properties", "Keycol=9999", "quoted_pipe.dat"))
      val tDF = context.table("quoted_data_tbl").filter(col("Keycol").equalTo(lit(9999)))
      tDF.show(false)
      tDF.count()
    }
  }


  test ("driver : Record filter test - select with start of record") {
    context.sql("drop table If exists multi_record_1_tbl")
    context.sql("CREATE TABLE multi_record_1_tbl(start_flag String,src_timestamp timestamp, src_date date, Keycol int, ValueCol string,end_flag String) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    assertResult(2) {
      Inferencer.main(Array("src/test/resources/appResources/multiRecod_1.properties", "", "multi_record.dat"))
      val tDF = context.table("multi_record_1_tbl")
      tDF.show(false)
      tDF.count()
    }
  }


  test ("driver : Record filter test - select with end of record") {
    context.sql("drop table If exists multi_record_2_tbl")
    context.sql("CREATE TABLE multi_record_2_tbl(start_flag String,src_timestamp timestamp, src_date date, Keycol int, ValueCol string,end_flag String) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    assertResult(2) {
      Inferencer.main(Array("src/test/resources/appResources/multiRecod_2.properties", "", "multi_record.dat"))
      val tDF = context.table("multi_record_2_tbl")
      tDF.show(false)
      tDF.count()
    }
  }

  test ("driver : Record filter test - select with start and end") {
    context.sql("drop table If exists multi_record_3_tbl")
    context.sql("CREATE TABLE multi_record_3_tbl(start_flag String,src_timestamp timestamp, src_date date, Keycol int, ValueCol string,end_flag String) " +
      "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    assertResult(1) {
      Inferencer.main(Array("src/test/resources/appResources/multiRecod_3.properties", "", "multi_record.dat"))
      val tDF = context.table("multi_record_3_tbl")
      tDF.show(false)
      tDF.count()
    }
  }
//  context.sql("SET spark.sql.parser.quotedRegexColumnNames=false")
//  context.sql("SET hive.support.quoted.identifiers=none")
//  context.sql("drop table If exists xmlTbl")
//  context.sql("""create table if not exists xmlTbl
//   (
//    tags String,
//    balance String,
//    picture String,
//    isActive String,
//    about String,
//    greeting String,
//    _id String,
//    `friends.id` String,
//    favoriteFruit String,
//    address String,
//    age String,
//    name String,
//    `friends.name` String,
//    dqvalidityFlag String,
//    guid String,
//    company String,
//    gender String,
//    eyeColor String,
//    registered String,
//    phone String,
//    index String,
//    longitude String,
//    email String,
//    latitude String
//  )""" + "STORED AS ORC LOCATION '/tmp/hive/" + randomUUID().toString + "'")
//  assertResult(56) {
//    driver.main(Array("src/test/resources/appResources/xmlData.properties", "", "randomData.xml"))
//    val tDF = context.table("xmlTbl")
//    tDF.show(false)
//    tDF.count()
//  }

  //Entry  Per. Post Date  GL Account   Description               Srce. Cflow  Ref.  Post             Debit              Credit  Alloc.
  test ("driver : Fixed width file with variable record header length ") {
    context.sql("drop table If exists fixed_width_sample")
    context.sql("""CREATE TABLE fixed_width_sample (entry string,
                  per string,
                  post_date date,
                   glaccount bigint,
                   description string,
                   srce string,
                   cflow string,
                   ref string,
                   post string,
                   debit string,
                   credit string,
                   alloc string
                   )"""+  "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    assertResult(48) {
      Inferencer.main(Array("src/test/resources/appResources/fixedwidthfileWithHeader.properties", "", "sample_data_20180520"))
      val tDF = context.table("fixed_width_sample")
      tDF.show(false)
      tDF.count()
    }
  }

  test ("driver : Fixed width file with variable record header length with custom header format ") {
    context.sql("drop table If exists headerFormated_tbl")
    context.sql("""CREATE TABLE headerFormated_tbl (entry string,
                  per string,
                  post_date date,
                   glaccount bigint,
                   description string,
                   srce string,
                   cflow string,
                   ref string,
                   post string,
                   debit string,
                   credit string,
                   alloc string,
                   cust_header_col string
                   )"""+  "STORED AS ORC LOCATION 'build/tmp/hive/" + randomUUID().toString + "'")
    assertResult(48) {
      Inferencer.main(Array("src/test/resources/appResources/headerFormat.properties", "", "headerTest.dat"))
      val tDF = context.table("headerFormated_tbl")
      tDF.show(false)
      tDF.count()
    }
  }
}
