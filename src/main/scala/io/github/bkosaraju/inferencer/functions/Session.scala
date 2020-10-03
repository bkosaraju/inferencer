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

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

trait Session  {
  /**Application logger
    * One place to define it and invoke every where.
     */
  def logger = LoggerFactory.getLogger(getClass)

  /**session
    * Spark session Creator this is the one place to create session, this can be used to define the custom properties for the application as well.
    * this also have the custom config such as
    * {{{
    *     config("hive.exec.dynamic.partition", "true")
    *     config("hive.exec.dynamic.partition.mode", "nonstrict")
    * }}}
    *here the spark session initiated with hive Support
    */
  def sparkSession : SparkSession =
    SparkSession
      .builder()
      .config("spark.sql.legacy.timeParserPolicy","legacy")
      .getOrCreate()
  //    .enableHiveSupport()
  //    .config("hive.exec.dynamic.partition", "true")
  //    .config("hive.exec.dynamic.partition.mode", "nonstrict")
}
