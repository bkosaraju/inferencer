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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

trait RemoveHeaderAndFooter extends Session {
  /**
    * Method to remove header(currently support only one line and should be text, fixedwidth files)
    * @param rdd : RDD[Row], readerOptions : Map[String,String]
    * @return rdd :RDD[Row]
    *
    */

  def removeHeaderAndFooter(rdd : RDD[String], readerOptions : Map[String,String]) : RDD[String] = {
    var defaultHeader, defaultFooter : Int = 0
    if (readerOptions.contains("header") && readerOptions("header").equals("true")) { defaultHeader = 1 }
    if (readerOptions.contains("footer") && readerOptions("footer").equals("true")) { defaultFooter = 1 }

    val headerLength = readerOptions.getOrElse("headerLength",defaultHeader).toString.toInt
    val footerLength = readerOptions.getOrElse("footerLength",defaultFooter).toString.toInt
    try {
      if ( headerLength  > 0 || footerLength > 0  ) {
        val dataSize = if (footerLength > 0 ) {rdd.count() } else { Long.MaxValue }
        rdd
          .zipWithIndex()
          .filter(itm => { itm._2 > headerLength -1 && itm._2 < dataSize - footerLength  }).map(_._1)
      } else { rdd }
    } catch {
      case e : Exception => {
        logger.error("Unable to remove header or/and footer from file!!",e)
        throw e
      }
    }
  }

}

