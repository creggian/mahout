/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.mahout.feature.mrmr.common.commandline;

import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;

public final class DefaultOptionCreator {
	
	public static final String TARGET_COLUMN = "target";
	public static final String TARGET_INDEX = "targetIndex";
	public static final String ROW_NUMBER = "numRows";
	public static final String COLUMN_NUMBER = "numCols";
	public static final String FEATURE_NUMBER = "numFeature";
	
	public static DefaultOptionBuilder targetColumnOption() {
		return new DefaultOptionBuilder()
        .withLongName(TARGET_COLUMN)
        .withRequired(true)
        .withShortName("t")
        .withArgument(
            new ArgumentBuilder().withName(TARGET_COLUMN)
                .withDefault("1")
                .withMinimum(1).withMaximum(1).create())
        .withDescription(
            "The column number of the target class");
	}
	
	public static DefaultOptionBuilder rowNumberOption() {
		return new DefaultOptionBuilder()
        .withLongName(ROW_NUMBER)
        .withRequired(true)
        .withShortName("nr")
        .withArgument(
            new ArgumentBuilder().withName(ROW_NUMBER).withMinimum(1)
                .withMaximum(1).create())
        .withDescription(
            "Number of rows in the dataset");
	}
	
	public static DefaultOptionBuilder columnNumberOption() {
		return new DefaultOptionBuilder()
        .withLongName(COLUMN_NUMBER)
        .withRequired(true)
        .withShortName("nc")
        .withArgument(
            new ArgumentBuilder().withName(COLUMN_NUMBER).withMinimum(1)
                .withMaximum(1).create())
        .withDescription(
            "Number of columns in the dataset");
	}
	
	public static DefaultOptionBuilder featureNumberOption() {
		return new DefaultOptionBuilder()
        .withLongName(FEATURE_NUMBER)
        .withRequired(true)
        .withShortName("nf")
        .withArgument(
            new ArgumentBuilder().withName(FEATURE_NUMBER).withMinimum(1)
                .withMaximum(1).create())
        .withDescription(
            "Number of feature to select");
	}
}
