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

package io.github.bkosaraju.inferencer.SchemaFlattnerTests

import io.github.bkosaraju.inferencer.jsonFlattenerTests
import io.github.bkosaraju.inferencer.{AppInterface, jsonFlattenerTests}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

trait GetArrayColListTests extends AppInterface {

   private val tschema = StructType(Seq(
    StructField("aElement1", ArrayType(StringType, true), true),
    StructField("aElement2", ArrayType(StringType, true), true),
    StructField("nonAElement", StringType, true))
  )

  test("getArrayColList : Test to Extract Arrays Items form Schema",jsonFlattenerTests) {
    assert(jf.getArrayColList(tschema).equals(Seq("aElement1", "aElement2")))
  }

}
