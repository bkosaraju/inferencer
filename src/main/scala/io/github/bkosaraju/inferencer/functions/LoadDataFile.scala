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

import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.util.matching.Regex


trait LoadDataFile
  extends LoadRdd
    with Session {
  /** Flat file loader, it loads file based out of the file and field separator record separator
    * this also cleanse any kind of special characters which are replaced in file with nothing.
    * this can read two kind of formats such binary and text wchi is subsequently passed to loadRDD method
    *
    * @return RDD of Row (org.apache.spark.sql.Row)
    * @example loadDataFile(path,recDelimiter,fieldDelimiter,cleanseChar,readerFormat)
    * @param path           path of the files where the method reads the datasets
    * @param recDelimiter   record delimiter to separate the records - best way is provide the record delimiters in unicode format
    *                       such as \\u4 digit numeric value
    *                       Regular expression is accepted so that multiple delimiters will be accepted.
    * @param fieldDelimiter field delimiter which separtes the records - same as the record delimiter its advise to provide them in unicode format.
    *                       Regular expression is accepted for the same
    * @param cleanseChar    clense char is used to clean the data from the file
    * @param readerFormat   source file format - eather of binary or text.
    *
    *                       {{{
    *                                             if ("".equals(cleanseChar)) {
    *                              loadRdd(path, readerFormat).flatMap(_._2.split(recDelimiter)).map(_.split(fieldDelimiter, -1))
    *                                .map(_.map(_.trim))
    *                                .map(x => Row.fromSeq(x))
    *                            } else {
    *                              loadRdd(path, readerFormat).flatMap(_._2.replaceAll(cleanseChar, "").split(recDelimiter)).map(_.split(fieldDelimiter, -1))
    *                                .map(_.map(_.trim))
    *                                .map(x => Row.fromSeq(x.map(_.trim)))
    *                            }
    *                       }}}
    *                  */
  def loadDataFile(path: String, recDelimiter: String = "\n", fieldDelimiter: String = ",", cleanseChar: String = "", readerFormat: String = "text", readerOptions : Map[String,String] = Map()): RDD[org.apache.spark.sql.Row] = {
    try {
      val rawData = loadRdd(path, readerFormat, recDelimiter,readerOptions)
    if ("".equals(cleanseChar)) {
      rawData
        .map(_._2.split(fieldDelimiter, -1))
        .map(_.map(_.trim.replaceAll("^\"(.*)\"$", "$1").trim))
        .map(_.map(x => if (x.isEmpty) null else x ))
        .map(x => Row.fromSeq(x))
    } else {
      rawData
        .map(_._2.replaceAll(cleanseChar, ""))
        .map(_.split(fieldDelimiter, -1))
        .map(_.map(_.trim.replaceAll("^\"(.*)\"$", "$1").trim))
        .map(_.map(x => if (x.isEmpty) null else x ))
        .map(x => Row.fromSeq(x))
    }
  } catch {
      case e : Exception => logger.error("Unable to Load File into RDD[Row]",e)
    throw e
    }
  }
}
