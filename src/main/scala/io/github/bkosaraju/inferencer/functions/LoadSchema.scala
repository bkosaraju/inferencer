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

import org.apache.spark.sql.types.StructType
import io.github.bkosaraju.utils.spark.ReadFromTable
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

trait LoadSchema extends Session with ReadFromTable {

  /**
    * Loads the source schema which is used to inferring schema to  source datasets.
    *
    * @param databaseName target database where the schema should be picked
    * @param tableName  target table name where the schema should be picked up
    * @return returns struct type.
    *         {{{      sparkSession.table(databaseName + "." + tableName).schema}}}
    */


  override def sparkSession: SparkSession = super[Session].sparkSession
  override def logger: Logger = super[Session].logger

  def loadSchema(databaseName: String, tableName: String): StructType = {
    try {
      sparkSession.table(databaseName + "." + tableName).schema
    } catch {
      case unKnownSchema: Exception =>
        logger.error("Unable to Load Schema from Source Table " + databaseName + "." + tableName, unKnownSchema)
        throw unKnownSchema
    }
  }

  def loadSchema(
                  databaseName: String,
                  tableName : String,
                  readerFormat: String,
                  readerOptions : Map[String,String]): StructType = {
    try {
    readFromTable(databaseName,tableName,readerFormat,readerOptions).schema
    } catch {
        case unKnownSchema: Exception =>
          logger.error("Unable to Load Schema from Source Table " + databaseName + "." + tableName, unKnownSchema)
          throw unKnownSchema
      }
      }
    }

