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

trait LoadFixedWidthFile
  extends LoadRdd
    with Session with Exceptions {
  /** Fixed width Flat file loader, it loads file based out of the file and recordLayout record separator
    * this also cleanse any kind of special characters which are replaced in file with nothing.
    * this can read two kind of formats such binary and text wchi is subsequently passed to loadRDD method
    *
    * @return RDD of Row (org.apache.spark.sql.Row)
    * @example loadDataFile(path,recDelimiter,fieldDelimiter,cleanseChar,readerFormat)
    * @param path           path of the files where the method reads the datasets
    * @param recDelimiter record delimiter to separate the records - best way is provide the record delimiters in unicode format
    *                     such as \\u4 digit numeric value
    *                     Regular expression is accepted so that multiple delimiters will be accepted.
    * @param recordLayout Record Layout informat of "StartPoss|length,StartPoss|length"
    *                     Regular expression is accepted for the same
    * @param cleanseChar  clense char is used to clean the data from the file
    *
    *                     {{{
    *                        var rowData = Seq.empty[String]
    *                            for (field <- recordLayout.split(",") ) {
    *                            val boundaries = field.trim.split('|').map(_.toInt -1 )
    *                            rowData = rowData :+ record.substring(boundaries(0),boundaries.sum+1 ).trim
    *                          }
    *                          Row.fromSeq(rowData)
    *                     }}}
    *
    */

  def loadFixedWidthFile(path: String, recordLayout: String = "", cleanseChar: String = "" , recDelimiter: String = "\n" ,readerOptions: Map[String,String] = Map()): RDD[org.apache.spark.sql.Row] = {

    val rowFromString = (record: String, recordLayout : String  ) => {
      var rowData = Seq.empty[String]
      for (field <- recordLayout.split(",") ) {
        val boundaries = field.trim.split('|').map(_.toInt)
        val currCol = record.substring(boundaries(0) -1 ,boundaries.sum -1 ).trim.replaceAll("^\"(.*)\"$", "$1").trim
         rowData = rowData :+ (if (currCol.isEmpty) null else currCol)
      }
      rowData
    }

    try {
      if (! recordLayout.contains('|')) {
        throw new UnsupportedRecordLayoutException("Illegal record layout specified please pass the layout in format of - StartPoss|length,StartPoss|length")
      } else {
        if ("".equals(cleanseChar)) {
          loadRdd(path,"","\n",readerOptions).map(x => Row.fromSeq(rowFromString(x._2,recordLayout.trim)))
        } else {
          loadRdd(path,"","\n",readerOptions).map(_._2.replaceAll(cleanseChar, "")).map(x => Row.fromSeq(rowFromString(x, recordLayout.trim)))
        }
      }
    } catch {
      case e : Exception => logger.error("Unable to Load File into RDD[Row]",e)
        throw e
    }
  }
}
