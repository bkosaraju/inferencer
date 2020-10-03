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

trait GetTargetValidationFilter extends Session {
  /** Filter Condition to validate the Target Column datatypes with non converted source DataTypes as part of the data type validation.
    * @return string of filter condition with casted datatypes.
    * @example getTargetValidationFilter(schema)
    * @param schema        target schema
    * {{{
    * schema.map(x => "coalesce(cast(".concat(x.name).concat(" as ").concat(x.dataType.typeName).concat("),'')=coalesce(").concat(x.name).concat(",'')")).toArray.mkString(" and \n")
    * }}}
    *                  */
  def getTargetValidationFilter(schema: StructType): String = {
    try {
    val optStr = schema.filter( col => ! Seq("date","timestamp","datetime","string").contains(col.dataType.typeName))
      .map(x =>
        "(`"
          .concat (x.name)
          .concat("` is null or (`")
        .concat (x.name)
          .concat("` is not null and  cast(`")
          .concat(x.name)
          .concat("` as ")
          .concat(x.dataType.typeName)
          .concat(")=`")
          .concat(x.name).concat("`))")
      ).toArray.mkString(" and \n")
      if (optStr.isEmpty){""} else " and ".concat(optStr)
  } catch {
    case e : Exception => logger.error("Unable to Generate Target Column Validation condition",e)
    throw e
    }
  }
}


