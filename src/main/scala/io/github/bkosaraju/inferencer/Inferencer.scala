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

package io.github.bkosaraju.inferencer

import java.util.{InputMismatchException, Properties}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import io.github.bkosaraju.utils.common.{LoadProperties => lp}
import io.github.bkosaraju.inferencer.functions.AppConfig

object Inferencer extends lp with AppConfig{
  val logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {

    if (args.length < 1 ) {
      logger.error(
        "Required Arguments Not Provided as (properties file, augmented column values, runId(optional) " +
      "- to derive the source directory path) but provided : "+args.mkString(","))
      throw new InputMismatchException(  "Required Arguments Not Provided as (properties file, augmented column values, runId(optional) " +
        "- to derive the source directory path) but provided : "+args.mkString(","))
    }

    val props = args(0).toString.loadParms()

    val spark = SparkSession
      .builder()
      .config(new SparkConf())
      .master(props.getProperty("mode","local"))
      .appName(props.getProperty("appName","inferencer"))
//      .enableHiveSupport()
      .getOrCreate()

    if (args.length > 1) {
      props.setProperty("dwsVars", args(1).toString)
    }
    val runId =
    if (args.length > 2 ) {
       args(2).toString
    } else {
      MANAGED_PROVIDER
    }
    props.setProperty("runId",runId)
    try {
      val loader = new LoadCuratedData
      loader.loadCuratedData(props)
    } catch {
      case e : Exception => {
        logger.error("Exception occurred while processing the data",e)
        throw e
      }
    }
  }
}
