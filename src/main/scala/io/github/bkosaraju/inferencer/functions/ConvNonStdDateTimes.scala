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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import java.lang.String

trait ConvNonStdDateTimes extends Session {
  /** Convert the Non standard DataTypes into standard timestamps in source Dataframe with the given input map of input datatype pattern
    * at the same time it converts the same dataframe into target data type
    *
    * @example convNonStdDateTimes(srcDF '''DataFrame''',soruceDatePattern '''Map of Column name to Column format''', tgtSchema '''Schema''' )
    * @param df        source dataframe where the columens need to be updated
    * @param dtTmCols  Key Value pair of the columns to be paaded to the dataframe
    * @param tgtSchema target Schema to extract and convert the target datatype
    * @return DataFrame of Format converted Dates & Timestamp
    *         {{{
    *          sDF
    *                  .withColumn(clmn,
    *                    from_unixtime(
    *                      unix_timestamp(
    *                        col(clmn),
    *                        dtTmCols(clmn))
    *                    ).cast(tgtSchema(clmn).dataType.typeName)
    *                  )
    *         }}}
    */

  def convNonStdDateTimes(df: DataFrame, dtTmCols: Map[String, String], tgtSchema: StructType, errorThresholdPercent: String ="100"): DataFrame = {
    try {
      val srcData = df.withColumn("dqvalidityFlag", lit("Y"))

      if (dtTmCols.nonEmpty ) {
        val flaggedRecDF =
          if ( errorThresholdPercent.toFloat < 100.00) {
          val flaggedRecDFextendedTypes = dtTmCols.filter(v => v._2.contains("S")).keys.toList.foldLeft(srcData)((sDF, clmn) => {
            sDF
              .withColumn("dqvalidityFlag",
                when(col(clmn).isNotNull and
                  concat(
                    from_unixtime(
                      unix_timestamp(
                        substring(trim(col(clmn)), 0, dtTmCols(clmn).indexOf("S")),
                        dtTmCols(clmn).substring(0, dtTmCols(clmn).indexOf("S"))
                      )
                    ), lit("."), trim(col(clmn)).substr(dtTmCols(clmn).indexOf("S") + 1, 1024)
                  ).cast(tgtSchema(clmn).dataType.typeName).isNull
                  , lit("N")
                ).otherwise(col("dqvalidityFlag")))
          }
          )
          dtTmCols.filter(v => ! v._2.contains("S")).keys.toList.foldLeft(flaggedRecDFextendedTypes)((sDF, clmn) => {
            sDF
              .withColumn("dqvalidityFlag",
                when(col(clmn).isNotNull && dtTmCols(clmn).equalsIgnoreCase("DEFAULT") && trim(col(clmn)).cast(tgtSchema(clmn).dataType.typeName).isNull,lit("N"))
                    .when(col(clmn).isNotNull &&
                  from_unixtime(
                    unix_timestamp(
                      trim(col(clmn)),
                      dtTmCols(clmn))
                  ).cast(tgtSchema(clmn).dataType.typeName).isNull
                  , lit("N"))
                  .otherwise(col("dqvalidityFlag")))
          }
          )
        } else srcData

         val extedendDF = dtTmCols.filter( v => v._2.contains("S"))
           .keys.toList
           .foldLeft(flaggedRecDF.filter(col("dqValidityFlag").equalTo(lit("Y"))))(
           (sDF, clmn) => {
          sDF
            .withColumn(clmn,
              concat(
              from_unixtime(
                  unix_timestamp(
                    substring(trim(col(clmn)),0,dtTmCols(clmn).indexOf("S")),
                    dtTmCols(clmn).substring(0,dtTmCols(clmn).indexOf("S"))
                  )
                ),lit("."),trim(col(clmn)).substr(dtTmCols(clmn).indexOf("S")+1,1024)
              ).cast(tgtSchema(clmn).dataType.typeName))}
         )

        dtTmCols.filter( v => ! v._2.contains("S")).keys.toList.foldLeft(extedendDF.filter(col("dqValidityFlag").equalTo(lit("Y"))))((sDF, clmn) => {
                sDF.withColumn(clmn,
                  when(col(clmn).isNotNull && dtTmCols(clmn).equalsIgnoreCase("DEFAULT"), trim(col(clmn)).cast(tgtSchema(clmn).dataType.typeName) )
                      .otherwise(
                from_unixtime(
                  unix_timestamp(
                    trim(col(clmn)),
                    dtTmCols(clmn))
                ).cast(tgtSchema(clmn).dataType.typeName)
              ))}
                )
          .union(flaggedRecDF.filter(col("dqValidityFlag").equalTo(lit("N"))))
      } else srcData
    } catch {
      case e: Exception => logger.error("Unable to Convert Non-Standard Data Types", e)
        throw e
    }
  }
}