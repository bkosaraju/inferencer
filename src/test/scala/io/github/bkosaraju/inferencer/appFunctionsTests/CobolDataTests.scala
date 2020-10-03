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

import java.util.{InputMismatchException, Properties}
import java.util.UUID.randomUUID

import io.github.bkosaraju.inferencer.Inferencer
import io.github.bkosaraju.inferencer.AppInterface
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit}

trait CobolDataTests extends AppInterface {

  private val hadoopfs =
    (new Path("src/test/resources/appResources/cobolApp.properties"))
    .getFileSystem(context.sessionState.newHadoopConf())
  context.sql("SET spark.sql.parser.quotedRegexColumnNames=false")
  context.sql("SET hive.support.quoted.identifiers=none")
  context.sql("Drop Table If exists tgtCobolTbl")
  context.sql(
    """CREATE  TABLE `tgtCobolTbl`(
      |`STATIC_DETAILS.ADDRESS` String,
      |`STATIC_DETAILS.COMPANY_NAME` String,
      |`STATIC_DETAILS.TAXPAYER.TAXPAYER_STR` String,
      |`COMPANY_ID` String,
      |`STATIC_DETAILS.TAXPAYER.TAXPAYER_TYPE` String,
      |`CONTACTS.PHONE_NUMBER` String,
      |`SEGMENT_ID` String,
      |`CONTACTS.CONTACT_PERSON` String,
      |`STATIC_DETAILS.TAXPAYER.TAXPAYER_NUM` Long)
      """.stripMargin+"STORED AS parquet LOCATION 'build/tmp/hive/"+randomUUID().toString+"'"
  )

  private val customPropsCobol = lp.loadCustomProperties(hadoopfs,"src/test/resources/appResources/cobolApp.properties",new Properties())

  test ("getCuratedDF : load the Cobol file to a dataframe") {
    assertResult(1000) {
      val DF = af.getCuratedDF("src/test/resources/cobolDataSets/COMP_DETAILS/COMP.DETAILS.SEP30.DATA.dat",customPropsCobol)(0)
      DF.printSchema()
      DF.show(10,false)
      DF.count()
    }
  }

}
