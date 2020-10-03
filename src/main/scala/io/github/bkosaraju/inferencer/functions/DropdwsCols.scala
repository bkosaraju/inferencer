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

trait DropdwsCols extends Session {
  /** Drop the existing data warehousing  / Audit columns to from the target schema to infer to source dataset.
    *
    * @example dropdwsCols(schema, processCols)
    *          @param schema        target Schema
    *          @param processCols   sequence of the columns to be dropped from target schema
    *          @return source schema StructType
    *
    * {{{StructType(schema.flatMap(x => if (!processCols.contains(x.name)) Some(x) else None)) }}}
    */
  def dropdwsCols(schema: StructType, processCols: Seq[String]): StructType = {
    try {
      StructType(schema.flatMap(x => if (!processCols.contains(x.name)) Some(x) else None))
    } catch {
      case e: Exception =>
        logger.error("Unable to Drop Extra Data Warehouse Columns from Schema.", e)
        throw e
    }
  }

}
