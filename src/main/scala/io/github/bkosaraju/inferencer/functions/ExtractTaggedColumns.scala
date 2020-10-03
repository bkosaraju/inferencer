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

import scala.util.matching.Regex

trait ExtractTaggedColumns  extends Session with Exceptions {

  /** Extract the tokens from FileName so that those can be padded to the target table incase of
    * need to be loaded into target table, this method takes three input parameters filename, file pattern and
    * token list out of that file name to be extracted
    *
    * @example extractTaggedColumns(filename, filePattern, tokenList)
    *          @param fileName     name of the file from which tokens should be extracted
    *          @param filePattern  Scala Regex pattern (as String) to apply on top of the pattern
    *          @param tokenList     array of string tobe extracted
    *          @return keyvalue pair of column values associated to parameters
    *          @throws  TokenCountException in case if file could not be fitted with the desired regex
    *
    * {{{StructType(schema.flatMap(x => if (!processCols.contains(x.name)) Some(x) else None)) }}}
    */

  def extractTaggedColumns(fileName : String, filePattern : String, tokenList : Array[String] ): Map[String,String] = {
    try {
      val matcher = new Regex(filePattern).findAllIn(fileName)
      matcher.hasNext //Initialize the matcher
      if (matcher.groupCount.equals(0)) {
        throw new PatternMismatchException("Pattern Not matched for the input file")
      } else {
        val tokenValues = for (i <- 1 to matcher.groupCount) yield matcher.group(i)
        if (matcher.groupCount < tokenList.length) {
          throw new TokenCountException("Number of tokens extracted(" + matcher.groupCount + ") are less than the number of tokens specified(" + tokenValues.length + ")")
        } else if (matcher.groupCount > tokenList.length) {
          logger.warn("more number of tokens detected in regex than the specified token count, " +
            "there may be chances that wrong values get parsed, for now considering only number of tokens")
          matcher.take(tokenValues.length)
        }
        (tokenList zip tokenValues).toMap
      }
    } catch {
      case e: Exception =>
        logger.error("Unable to Extract Tokens from FileName : "+ fileName + "with Regex :"+ filePattern, e)
        throw e
    }
  }
}