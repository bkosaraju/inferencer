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

trait SrcToTgtColSchemaChange extends Session {
  /**
    * Schema to change the columns from source to target, this will be usefull incases of schema driven sources.
    *
    * @param schema StructType
    * @param mappings source to Target column mappings
    * @return Schema
    *         {{{val  rMappings = mappings.map(_.swap)
    *                  StructType(schema.flatMap(x => if (rMappings.contains(x.name)) Some(x.copy(name = rMappings(x.name))) else Some(x)))}}}
    */
  def srcToTgtColSchemaChange(schema: StructType, mappings: Map[String, String] = Map()): StructType = {
    if (mappings.nonEmpty) {
      try {
        val  rMappings = mappings.map(_.swap)
        StructType(schema.flatMap(x => if (rMappings.contains(x.name)) Some(x.copy(name = rMappings(x.name))) else Some(x)))
      } catch {
        case e: Exception =>
          logger.error("Unable to Transpose Source Columns into Target Columns", e)
          throw e
      }
    } else
      schema
  }
}