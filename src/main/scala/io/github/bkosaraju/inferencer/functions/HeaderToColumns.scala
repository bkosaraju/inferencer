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

import java.util.Properties

trait HeaderToColumns
  extends Session
  with StringToMap
  with ExtractTaggedColumns
  with Exceptions {
  /**
    * Custom Column mapper
    * this is predominantly used across sourec to target column renames, source datatypes of timestamp and date fields in along the ammending the datawarehousing fileds.
    *
    * @param path input path for the source file
    * @param props properties of given input file to process
    * @return Column mapped values of strings to load into table
    *
    */
  def headerToColumns(path: String, props : Properties): Map[String,String] = {
    try {
    val readerOptions = stringToMap(props.getProperty("readerOptions",""))
    val headerFormat = props.getProperty("headerFormat","")
    val headerColumns = props.getProperty("headerColumns","").split(",")
    if (headerFormat.equals("") || headerColumns.equals("")) {
      throw HeaderFormatException("headerFormat / headerColumns not passed for the parser to read header columns")
    }
    val header = sparkSession.read.textFile(path).take(readerOptions("headerLength").toInt).mkString("\n")
    extractTaggedColumns(header,headerFormat,headerColumns)
    } catch {
      case e: Exception =>
        logger.error("Unable to Parse the header into custom coulmns ", e)
        throw e
    }
  }

}
