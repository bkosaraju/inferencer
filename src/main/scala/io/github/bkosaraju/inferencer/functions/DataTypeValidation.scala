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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col, lit, trim}

import scala.util.Try


trait DataTypeValidation
  extends Session
    with GetTargetValidationFilter
    with GetTargetSelList
    with Exceptions
    with StringToMap {
  /** DataType Validator will check for data Intigrity with the specified data.
    *
    * @return Some of DataFrame or invalidDataThresholdException..
    * @example dataTypeValidation(sourceDF,ConvertedSchema,Properties)
    * @param srcDF : Souce DataFrame
    *        ConvSchema : ConvertedSchema,
    *        props : Properties
    **/


  def dataTypeValidation(convSrcDttmDF : DataFrame, srcDF: DataFrame, srcConvSchema: StructType, props: Properties): Array[DataFrame] = {

    val errorThreshold = props.getProperty("errorThresholdPercent", "100").replaceAll("%", "")
    val custTransMap = stringToMap(props.getProperty("customTransformationOptions",""))
    convSrcDttmDF.createOrReplaceTempView("stgData")
    if (errorThreshold.toFloat >= 100 ) {
      val VALID_DATA_SQL = "select " + getTargetSelList(srcConvSchema,custTransMap) + " From stgData"
      val validatedDF = sparkSession.sql(VALID_DATA_SQL)
      val invalidDataDF = sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], validatedDF.schema)
      Array(validatedDF,invalidDataDF)
    }
    else {
      val VALID_DATA_SQL = "select " + getTargetSelList(srcConvSchema) + " From stgData" + " where dqValidityFlag='Y'  " + getTargetValidationFilter(srcConvSchema)
      val ORIG_VALID_DATA_SQL = "select * from stgData  where dqValidityFlag='Y'  " + getTargetValidationFilter(srcConvSchema)
      //val INVALID_DATA_SQL = "select * From stgData" + " where not( dqValidityFlag='Y' and " + getTargetValidationFilter(srcConvSchema)+")"
      val validatedDF = sparkSession.sql(VALID_DATA_SQL)
      val invalidDataDF =  convSrcDttmDF.except(sparkSession.sql(ORIG_VALID_DATA_SQL)).drop("dqvalidityFlag")
      //sparkSession.sql(INVALID_DATA_SQL).drop("dqvalidityFlag")
      val totalRecCount = try {
        srcDF.count()
      } catch {
        case e : Exception => logger.error("\nUnable to load the source file to Dataframe!! " +
          "\n This is mostly due to the file schema is not matching with target schema :'( \n" +
          "please check source(file) and target(table specified at tgtTable) accordingly\n")
          throw e
      }
        val validReccount = validatedDF.count()
        val deviation : Float = Try(((totalRecCount - validReccount) * 100 / totalRecCount).toFloat).getOrElse(0)
        if ( deviation <= errorThreshold.toFloat ) {
          logger.info("Number of records received from source : "+totalRecCount +"\nValid Record count : "+ validReccount+"\nProceeding for writing data into target location")
          Array(validatedDF,invalidDataDF)
        } else {
          throw new ErrorRecordThresholdException("Data has reached the threshold error record deviation !!\n" +
            "source has : " + totalRecCount + " records that inculdes : " +
            (totalRecCount - validReccount) +
            " of invalid records !! \n" +
            "which("+deviation+") is more than threshold specified : " + errorThreshold+"% hence")
        }
      }
    }
}
