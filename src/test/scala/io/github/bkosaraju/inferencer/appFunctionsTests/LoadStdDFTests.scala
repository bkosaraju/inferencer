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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, _}

trait LoadStdDFTests extends AppInterface {

  private val dfSchema = StructType(Seq(
    StructField("src_timestamp", TimestampType , true),
    StructField("src_date", DateType, true),
    StructField("Keycol", IntegerType, true),
    StructField("ValueCol", StringType, true))
  )

  private val srcSchema = StructType(Seq(
    StructField("src_timestamp", StringType , true),
    StructField("src_date", StringType, true),
    StructField("Keycol", StringType, true),
    StructField("ValueCol", StringType, true))
  )

  test ("loadStdDF : load the CSV file to a dataframe - record Count",appFunctionTests) {
    val df = af.loadStdDF("src/test/resources/csvdatasets/rand_data.csv","csv",Map("header" -> "true"),srcSchema)
    df.show(10,false)
    assertResult(500) {
      df.count
    }
  }


  test ("loadStdDF : load the Json file to a dataframe",appFunctionTests) {
    val df = af.loadStdDF("src/test/resources/jsondatasets/randomData.json","json",Map("multiLine" -> "true"),srcSchema)
    df.schema
    df.show(10)
      assertResult(6) {
      df.count
    }
  }

  private val tgtInferschema = StructType(Seq(
    StructField("src_timestamp", StringType , true),
    StructField("src_date", StringType, true),
    StructField("Keycol", StringType, true),
    StructField("ValueCol", StringType, true),
    StructField("extraCol", StringType, true))
  )

  test ("loadStdDF : load the Json file to a dataframe using inferTargetSchema option",appFunctionTests) {
    val df = af.loadStdDF("src/test/resources/jsondatasets/josnDateTimeColMap.json","json",Map("multiLine" -> "true","inferTargetSchema"->"true"),tgtInferschema,tgtInferschema)
    df.schema
    df.show(10)
    assertResult(2) {
      df.count
    }
  }

  test ("loadStdDF : load the orc file to a dataframe",appFunctionTests) {
    val df = af.loadStdDF("src/test/resources/orcdatasets/randdata.orc","orc",Map(),srcSchema,srcSchema)
    df.schema
    df.show(10)
    assertResult(500) {
      df.count
    }
  }


  private val srcDf = af.loadStdDF("src/test/resources/csvdatasets/rand_data.csv","csv",Map("header" -> "true"),srcSchema)

  test("loadStdDF : load the CSV file to a dataframe - Null values check",appFunctionTests) {
    assertResult(0) {
      srcDf.filter(col("src_timestamp").isNull or col("src_date").isNull or col("Keycol").isNull or col("ValueCol").isNull).count()
    }
  }
  test("loadStdDF : Unable to load the data into dataframe and throws exception in case if input schema is differ to the specified schema", appFunctionTests) {
    intercept[SchemaMismatchException] {
      af.loadStdDF("src/test/resources/orcdatasets/randdataSchemaChange.orc","orc",Map(),srcSchema)
    }
  }

  test("loadStdDF : load CSV file with no reader options", appFunctionTests) {
    assertResult(501) {
      af.loadStdDF("src/test/resources/csvdatasets/rand_data.csv","csv",Map(),srcSchema).count()
    }
  }
}
