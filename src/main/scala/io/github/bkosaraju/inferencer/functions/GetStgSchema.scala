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

import org.apache.spark.sql.types.{StringType, StructType}

trait GetStgSchema
  extends DropdwsCols
  with Session
  with SrcToTgtColSchemaChange
{
  /** Derives the source Schema from target Schema
    * returns two Structures
    * 1. Target schema with dropped audit columns in along with all the columns datatypes converted the string type
    * 2. Target Schema with dropped audit columns, in addition to any columns which have columns renamed from source to target
    *     this will be helpful in case of schema intern source datasets such as Json/Avro/ORC/Parquet formats, in this perticular case source datatypes are retained.
    *
    * @example getStgSchema(schema, processCols,mappings)
    *          @param schema        target schema
    *          @param processCols   sequence of the columns to be dropped from target schema
    *          @param mappings[optional] Any of the source to target mappings
    *          @return Array of source schema StructType
    *                  {{{
    *                        StructType(dropdwsCols(schema, processCols).map(elmnt => elmnt.copy(dataType = StringType))),
    *                           StructType(srcToTgtColSchemaChange(dropdwsCols(schema, processCols), mappings)))
    *                           }}}
    *                  */
  def getStgSchema(schema: StructType, processCols: Seq[String], mappings: Map[String, String] = Map()): Array[StructType] = {
    try {
      Array(
        StructType(srcToTgtColSchemaChange(StructType(dropdwsCols(schema, processCols).map(elmnt => elmnt.copy(dataType = StringType))),mappings)),
        //StructType(srcToTgtColSchemaChange(dropdwsCols(schema, processCols), mappings)))
        StructType(srcToTgtColSchemaChange(schema, mappings)))
    } catch {
      case e: Exception =>
        logger.error("Unable to Load the Staging Schema from Curated Schema", e)
        throw e
    }
  }

}
