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

import scala.collection.mutable

trait LoadTableVarsTests extends AppInterface {

  test ("loadTableVars : extract the table propertise from catalogue",appFunctionTests) {
    val tblprops = mutable.LinkedHashMap("Location" ->"hdfs://testcluster/data/dblocation/tbllocation",
      "Serde Library" -> "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
      "Some Property" -> "Some prop Value",
      "Another Property" -> "other Value")

    assertResult(Array("hdfs://testcluster/data/dblocation/tbllocation","parquet")){
      af.loadTableVars(tblprops).values.toArray
    }
  }

  test("loadTableVars : extarct table properties from catalogue - exception-parquet", appFunctionTests) {
    assertThrows[NoSuchElementException]{
      af.loadTableVars(mutable.LinkedHashMap[String,String]())
    }
  }
  val tblpropsjson = mutable.LinkedHashMap("Location" ->"hdfs://testcluster/data/dblocation/tbllocation",
    "Serde Library" -> "org.apache.hive.hcatalog.data.JsonSerDe",
    "Some Property" -> "Some prop Value",
    "Another Property" -> "other Value")

  test("loadTableVars : extarct table properties from catalogue - exception - jsonFormat", appFunctionTests) {
    assertResult(Array("hdfs://testcluster/data/dblocation/tbllocation","json")){
      af.loadTableVars(tblpropsjson).values.toArray
    }
  }

  val tblpropsavro = mutable.LinkedHashMap("Location" ->"hdfs://testcluster/data/dblocation/tbllocation",
    "Serde Library" -> "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
    "Some Property" -> "Some prop Value",
    "Another Property" -> "other Value")

  test("loadTableVars : extarct table properties from catalogue - exception - Avor Format", appFunctionTests) {
    assertResult(Array("hdfs://testcluster/data/dblocation/tbllocation","com.databricks.spark.avro")){
      af.loadTableVars(tblpropsavro).values.toArray
    }
  }

  val tblpropscsv = mutable.LinkedHashMap("Location" ->"hdfs://testcluster/data/dblocation/tbllocation",
    "Serde Library" -> "org.apache.hadoop.hive.serde2.OpenCSVSerde",
    "Some Property" -> "Some prop Value",
    "Another Property" -> "other Value")

  test("loadTableVars : extarct table properties from catalogue - exception - csv Format", appFunctionTests) {
    assertResult(Array("hdfs://testcluster/data/dblocation/tbllocation","csv")){
      af.loadTableVars(tblpropscsv).values.toArray
    }
  }

  val tblpropsTextFile = mutable.LinkedHashMap("Location" ->"hdfs://testcluster/data/dblocation/tbllocation",
    "Serde Library" -> "org.apache.hadoop.hive.serde2.RegexSerDe",
    "Some Property" -> "Some prop Value",
    "Another Property" -> "other Value")

  test("loadTableVars : extarct table properties from catalogue - exception - text Format", appFunctionTests) {
    assertResult(Array("hdfs://testcluster/data/dblocation/tbllocation","textfile")){
      af.loadTableVars(tblpropsTextFile).values.toArray
    }
  }


  val unknowProps = mutable.LinkedHashMap("Location" ->"hdfs://testcluster/data/dblocation/tbllocation",
    "Serde Library" -> "org.apache.some.unknow.data",
    "Some Property" -> "Some prop Value",
    "Another Property" -> "other Value")

  test("loadTableVars : Unable to load the table properties and throws exception in case if provided serde library is not in listed serde") {
    intercept[Exception] {
      af.loadTableVars(unknowProps)
    }
  }
}
