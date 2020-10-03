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

trait WriteHiveTable
  extends Session
    with LoadTableVars
    with RemoveHdfsData {
  /**
    * Writes the data Into Hive table
    * this is the highlevel API which will write to target specific data formats.
    * this has some of the lowlevel API call which will be described in other sections.
    * @param df dataframe to be wirtten
    * @param hiveDB databse where target table resides
    * @param hiveTable target table which should be loaded
    * @param writeMode write mode - append or overwrite
    * @param partitionCols partition columns if you have any
    */
  def writeHiveTable(tDf: DataFrame, hiveDB: String, hiveTable: String, writeMode: String, partitionCols : String = "", curationFlag : String = "",dataSize : Int = 1 ): Unit = {
    try {
      val df = tDf.repartition(dataSize)
      val tableVars = loadTableVars(sparkSession.sharedState.externalCatalog.getTable(hiveDB, hiveTable).toLinkedHashMap)
      if (curationFlag.equals("N")) {
        val eDf =
          if (tableVars("tableFormat").toLowerCase().contains("orc") || tableVars("tableFormat").toLowerCase().contains("parquet")) {
            df.schema.map(col => col.name).foldLeft(df)(
              (df, col) => df.withColumnRenamed(
                col,
                col.replaceAll("[ ,;{}()\\n\\t=]", "_")
              )
            )
          } else { df }
        eDf.write.option("path", tableVars("location").replaceAll("/$","")+".err")
          .format(tableVars("tableFormat"))
          .mode("append")
          .save
      } else {
        logger.info("Started loading the data into " + hiveDB + "." + hiveTable)
        if (writeMode.toLowerCase().equals("append")) {

          df.write.mode(writeMode).insertInto(hiveDB + "." + hiveTable)
        } else {

          removeHdfsData(tableVars("location"))

          if ("".equals(partitionCols)) {
            df
              .write.option("path", tableVars("location"))
              .format(tableVars("tableFormat"))
              .save
          } else {
            //remove existing partitions from path.
            sparkSession.sharedState.externalCatalog.dropPartitions(
              hiveDB,
              hiveTable,
              sparkSession.sharedState.externalCatalog.listPartitions(hiveDB,hiveTable).map(_.spec),
              true,
              true,
              true)

            df.write.mode(writeMode).insertInto(hiveDB + "." + hiveTable)

          }
        }
        logger.info("Successfully loaded the table : " + hiveDB + "." + hiveTable)
      }
    } catch {
        case e: Throwable =>
          logger.error("Unable to Write Data to Hive Table : " + hiveDB + "." + hiveTable + "\n", e)
        throw e
      }
    }
}
