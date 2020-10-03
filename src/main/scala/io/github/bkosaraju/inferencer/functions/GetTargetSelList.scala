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

trait GetTargetSelList extends Session {
  /** Obtarins the Target Column List with converted DataTypes to get matched dataframe with is exact to targetTable(curated)
    * @return string of select column list with casted datatypes.
    * @example getTargetSelList(schema)
    * @param schema        target schema
    *                  {{{
    *                  schema.map(x => "cast(".concat(x.name).concat(" as ").concat(x.dataType.typeName).concat(")")).toArray.mkString(",\n")
    *                           }}}
    *                  */
  def getTargetSelList(schema: StructType, custTransMap : Map[String,String] = Map()): String = {
    val primitiveTypes = Seq("IntegerType","LongType","FloatType","DoubleType","DecimalType","StringType","BinaryType","TimestampType","DateType")

    try {
      schema.map(x =>
        if (custTransMap.keySet.contains(x.name)) {
          custTransMap(x.name).concat (" as ").concat(x.name)
        } else if (primitiveTypes.contains(x.dataType.toString)) {
        "cast(`".concat(x.name).concat("` as ").concat(x.dataType.typeName).concat(")")
      } else "`"+x.name+"`").toArray.mkString(",\n")
    }catch {
      case e : Exception => logger.error("Column List Does Not Match Target Column List",e)
        throw e
    }
  }
}
