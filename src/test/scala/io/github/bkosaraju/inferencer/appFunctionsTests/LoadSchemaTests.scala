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

import java.io.FileReader
import java.util.UUID.randomUUID

import io.github.bkosaraju.inferencer.appFunctionTests
import org.h2.tools.{RunScript, Server}
import org.h2.jdbcx.JdbcDataSource
import java.util.Properties

import io.github.bkosaraju.inferencer.{AppInterface, appFunctionTests}

trait LoadSchemaTests extends AppInterface {

  private val orcTbl = "a"+randomUUID().toString.substring(0,4)
  context.sql("CREATE  TABLE if not exists "+orcTbl+"(src_timestamp string, src_date string, Keycol string, ValueCol string) STORED AS ORC LOCATION 'build/tmp/hive/"+randomUUID().toString+"'")

  test("loadSchemaTests : able to extract the schema from a catalogue  ",appFunctionTests) {
    assert(af.loadSchema("default",orcTbl).fieldNames.length.equals(4))
  }

  test("loadSchema : Unable to load schema and throws exception for given table if a table and database not existed", appFunctionTests) {
    intercept[Exception] {
      af.loadSchema("someDB","SomeUnKnowTable")
    }
  }


  test("loadSchema : Unable to load schema and throws exception for given table if a table not existed", appFunctionTests) {
    intercept[Exception] {
      af.loadSchema("default","SomeUnKnowTable")
    }
  }


  test("loadSchema : Unable to load schema and throws exception for given table if a database not existed", appFunctionTests) {
    intercept[Exception] {
      af.loadSchema("someUnknownDB",orcTbl)
    }
  }

  test("loadSchema: able to create H2 DB Instance and create table for testing purpose") {
    assertResult(null) {
      val ds = new JdbcDataSource()
      ds.setURL("jdbc:h2:mem:sparkTests")
      ds.setPassword("")
      ds.setUser("sa")
      val connection = ds.getConnection
      RunScript.execute(connection, new FileReader(("src/test/resources/H2/sparkTests.DDL")))
    }
  }

  test("loadSchema: able to read data from RDBMS Data source") {
    val databaseReaderOptions = Map("user"->"sa","password"->"","url" -> "jdbc:h2:mem:sparkTests")
    assertResult("StructType(StructField(EMP_NO,IntegerType,true), StructField(BIRTH_DATE,DateType,true), StructField(FIRST_NAME,StringType,true), StructField(LAST_NAME,StringType,true), StructField(GENDER,StringType,true), StructField(HIRE_DATE,DateType,true))"
    ) {
      af.loadSchema("sparkTests","employees","jdbc",databaseReaderOptions).toString()
    }
  }

  test("loadSchema: Exception while unable to read schema from Database") {
    val databaseReaderOptions = Map("user"->"sa","password"->"wrongpassword","url" -> "jdbc:h2:mem:sparkTests")
    intercept[Exception] {
      af.loadSchema("sparkTests","employees","jdbc",databaseReaderOptions).toString()
    }
  }
}
