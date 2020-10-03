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

import io.github.bkosaraju.inferencer.LoadProperties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, explode, lit, size}
import org.apache.spark.sql.types.StructType

class SchemaFlattener() extends LoadProperties with Exceptions {

  def flattenColList(schema: StructType,pfx :String = ""): Array[String] = {
    if (schema.nonEmpty) {
      schema.fields.flatMap { f =>
        f.dataType match {
          case struct: StructType  => flattenColList(struct, pfx + "`" +f.name+ "`" + ".")
          case _ => Array(pfx + "`" +f.name+ "`")
          }
      }
    } else {
      logger.error("Empty Schema passed")
      throw new EmptySchemaException("Empty Schema passed")
    }
  }


  def getArrayColList (schema : StructType) : Seq[String]  = {
    try {
      schema.flatMap(f => if (f.dataType.typeName.equals("array")) Some(f.name) else None)
    } catch {
      case e : Exception =>logger.error("Unable to Get Array Column List")
        throw e
    }
  }

  def explodeDF ( df : DataFrame ) : DataFrame = {
    try {
      val cleanDF = df //clearArrayNullColumns(df)
      val rDF = getArrayColList(df.schema).foldLeft(cleanDF) {
        (sDF, clmn) =>
          sDF.filter(col(clmn).isNull).withColumn(clmn, lit(null))
            .union(sDF.filter(size(col(clmn)).equalTo(0)).withColumn(clmn, lit(null)))
            .union(sDF.filter(col(clmn).isNotNull).withColumn(clmn, explode(col(clmn))))

      }
      val structCols = rDF.schema.flatMap(x => if (x.dataType.typeName.equals("struct")) Some(x) else None)
      if (structCols.nonEmpty) {
        flatStruct(rDF)
      } else rDF
    } catch {
      case e : Exception => logger.error("Unable to Explode Dataframe",e)
        throw e
    }
  }

  def flatStruct(jDF : DataFrame, nestedColSep: String = "") : DataFrame = {
    try {
    val colList = for (colName <- flattenColList(jDF.schema,nestedColSep) ) yield colName + " as `"+ colName.replaceAll("`","") +"`"
    jDF.createOrReplaceTempView("jTempTable")
    val fDF = sparkSession.sql("select "+colList.mkString(",")+" from jTempTable")
    explodeDF(fDF)
  } catch {
      case e : Exception => logger.error("Unable to Flatten Column List",e)
      throw e
    }
  }

}

object SchemaFlattener {
  def apply(dataFrame: DataFrame,netstedColSep: String): DataFrame = (new SchemaFlattener()).flatStruct(dataFrame)
}
