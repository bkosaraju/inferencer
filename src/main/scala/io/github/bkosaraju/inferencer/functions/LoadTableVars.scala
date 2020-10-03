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

import scala.collection.mutable

trait LoadTableVars extends Session
      with Exceptions{
  /**
    * Loads the Hive table properties into a Map and convert them into spark usable format.
    * depends upon the table serde this will converts into required format and get the table location as well.
    *
    * @param configMap Linked Hash map of the table properties
    * @return map of Desired table properties
    *         {{{
    *               Map(
    *                "location" -> configMap("Location"),
    *                "tableFormat" -> {
    *                  if (tableSerDe.contains("orc")) "orc"
    *                  else if (tableSerDe.contains("parquet")) "parquet"
    *                  else if (tableSerDe.contains("avro")) "com.databricks.spark.avro" //TODO : If the format changes
    *                  else if (tableSerDe.contains("json")) "json"
    *                  else if (tableSerDe.contains("csv")) "csv"
    *                  else if (tableSerDe.contains("regexserde")) "textfile"
    *         }}}
    *
    */
  def loadTableVars (configMap : mutable.LinkedHashMap[String,String]) : Map[String,String] = {
    val tableSerDe = configMap("Serde Library").toLowerCase
    Map(
      "location" -> configMap("Location"),
      "tableFormat" -> {
        if (tableSerDe.contains("orc")) "orc"
        else if (tableSerDe.contains("parquet")) "parquet"
        else if (tableSerDe.contains("avro")) "com.databricks.spark.avro" //TODO : If the format changes
        else if (tableSerDe.contains("json")) "json"
        else if (tableSerDe.contains("csv")) "csv"
        else if (tableSerDe.contains("regexserde")) "textfile"
        else {
          logger.error("Unknown Table Format -  currently the module only supports Orc,Parquet,Avro(com.databricks.spark.avro),json,CSV,textFile formats\n" +
            "Requested Serde Library is "+ tableSerDe)
          throw new UnsupportedFileformatException("Unknown Table Format - currently the module only supports Orc,Parquet,Avro(com.databricks.spark.avro),json,CSV,textFile formats\n" +
            "Requested Serde Library is "+ tableSerDe)
        }
      }
    )
  }
}
