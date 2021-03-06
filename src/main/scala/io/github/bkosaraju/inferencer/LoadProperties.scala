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

import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

class LoadProperties( ) extends AppFunctions {

  override val logger = LoggerFactory.getLogger(classOf[AppFunctions])

  def loadLocalappProps(appProps : String = "app.properties") : Properties = {

    val prop = new Properties()
    try {
      val internalpops = getClass.getClassLoader.getResourceAsStream(appProps)
      prop.load(internalpops)
    } catch {
      case e: Throwable => {
        logger.warn("Unable to Load Internal Properties", e)
      }
    }
    prop
  }

  def loadCustomProperties (hadoopfs : FileSystem, propfl : String ="", prop: Properties) : Properties = {

    //overwrite with custom values from HDFS Spark

    if ( propfl.length != 0 ) {
      val hadoopfsStreem = hadoopfs.open(new Path(propfl))
      prop.load(hadoopfsStreem)
    }
    else {
      logger.warn ("No Configuration File Provided")
    }
    prop
  }

}