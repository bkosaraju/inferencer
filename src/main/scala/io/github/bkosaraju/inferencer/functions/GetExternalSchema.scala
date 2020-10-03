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

import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters


trait GetExternalSchema extends Session {

  def getExternalSchema(schemaFile: String): StructType = {

    try {
      val schemaURI = new Path(schemaFile.replaceAll("s3://","s3a://"))
      val hadoopfs = schemaURI.getFileSystem(sparkSession.sessionState.newHadoopConf())
      val inputSchema = IOUtils.toString(hadoopfs.open(schemaURI), StandardCharsets.UTF_8)
      if (schemaFile.toLowerCase.contains(".avsc")) {
        val schema = new Schema.Parser().parse(inputSchema)
        SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
      } else if (schemaFile.toLowerCase.contains(".json")) {
        DataType.fromJson(inputSchema).asInstanceOf[StructType]
      } else {
        StructType.fromDDL(inputSchema)
      }
    } catch {
      case e: Exception => {
        logger.error(s"Unable to read given InuptSchema from ${schemaFile}")
        throw e
      }
    }
  }
}

