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

package io.github.bkosaraju.inferencer.functions

import org.apache.spark.sql.DataFrame

trait LoadTableandRegister extends Session  {
  /**
    * loads the hive table into a dataframe
    *
    * @param hiveDatabase
    * @param hiveTable
    * @param tempView optional argument so that you can use it is as spark temp view for SQL operations.
    * @return dataframe
    *         {{{
    *                 val tDF = sparkSession.table(hiveDatabase + "." + hiveTable)
    *                tDF.createOrReplaceTempView(rtmpView)
    *                tDF
    *         }}}
    */
  def loadTableandRegister(hiveDatabase: String, hiveTable: String, tempView: String = ""): DataFrame = {
    val rtmpView = if (tempView.length == 0) hiveTable else tempView
    try {
      val tDF = sparkSession.table(hiveDatabase + "." + hiveTable)
      tDF.createOrReplaceTempView(rtmpView)
      tDF
    } catch {
      case e: Throwable =>
        logger.error("Unable to Load Dataframe for Given Path to Hive : " + hiveDatabase + "." + hiveTable, e)
        throw e
    }
  }
}
