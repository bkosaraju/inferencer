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

import io.github.bkosaraju.inferencer.AppInterface

trait GetExternalSchemaTests extends AppInterface{

  test ("GetExternalSchemaTests : ReadSchema from DDL") {
   val  schemaFile = "src/test/resources/appResources/externalSchema.schema"
    assertResult("StructType(StructField(per,StringType,true), StructField(post_date,DateType,true), StructField(accountId,LongType,true), StructField(deviation,DecimalType(10,2),true))") {
      af.getExternalSchema(schemaFile).toString()
    }
  }

  test ("GetExternalSchemaTests : ReadSchema from AvroSchema") {
    val  schemaFile = "src/test/resources/appResources/externalSchema.avsc"
    assertResult("StructType(StructField(username,StringType,false), StructField(age,IntegerType,false), StructField(phone,StringType,false), StructField(housenum,StringType,false), StructField(address,StructType(StructField(street,StringType,false), StructField(city,StringType,false), StructField(state_prov,StringType,false), StructField(country,StringType,false), StructField(zip,StringType,false)),false))") {
      af.getExternalSchema(schemaFile).toString()
    }
  }


  test ("GetExternalSchemaTests : ReadSchema from JsonSchema") {
    val  schemaFile = "src/test/resources/appResources/externalSchema.json"
    assertResult("StructType(StructField(username,StringType,false), StructField(age,IntegerType,false), StructField(phone,StringType,false), StructField(housenum,StringType,false), StructField(address,StructType(StructField(street,StringType,false), StructField(city,StringType,false), StructField(state_prov,StringType,false), StructField(country,StringType,false), StructField(zip,StringType,false)),false))") {
      af.getExternalSchema(schemaFile).toString()
    }
  }

  test("GetExternalSchemaTests : Raise Exception incase if not able to produce schema for given input") {
    intercept[Exception] {
     val schemaFile = ""
      af.getExternalSchema(schemaFile).toString()
    }
  }

}
