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
package org.apache.mahout.feature.mrmr;

import org.apache.mahout.feature.mrmr.common.commandline.DefaultOptionCreator;
import org.apache.mahout.feature.mrmr.common.mapreduce.MaxCombiner;
import org.apache.mahout.feature.mrmr.common.mapreduce.MaxReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

import java.lang.NumberFormatException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MRMRDriver extends AbstractJob {
	
	private static final Logger log = LoggerFactory.getLogger(MRMRDriver.class);
	
	private String featureJobTempUri(Path basedir, int iteration) {
		return basedir.toUri() + Path.SEPARATOR + iteration + "_feature";
	}
	
	private String maxJobTempUri(Path basedir, int iteration) {
		return basedir.toUri() + Path.SEPARATOR + iteration + "_max";
	}
	
	private String cacheFileUri(Path basedir, int iteration) {
		return basedir.toUri() + Path.SEPARATOR + (iteration-1) + "_max" + Path.SEPARATOR + "part-r-00000";
	}
	
	public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MRMRDriver(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
		
		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.targetColumnOption().create());
		addOption(DefaultOptionCreator.rowNumberOption().create());
		addOption(DefaultOptionCreator.columnNumberOption().create());
		addOption(DefaultOptionCreator.featureNumberOption().create());
		
		Map<String,List<String>> parsedArgs = parseArguments(args);
		
		Path input = getInputPath();
		Path output = getOutputPath();
		Path temp = getTempPath();
		
		int targetColumn = Integer.parseInt(getOption(DefaultOptionCreator.TARGET_COLUMN));
		int rowNumber = Integer.parseInt(getOption(DefaultOptionCreator.ROW_NUMBER));
		int columnNumber = Integer.parseInt(getOption(DefaultOptionCreator.COLUMN_NUMBER));
		int featureNumber = Integer.parseInt(getOption(DefaultOptionCreator.FEATURE_NUMBER));
		
		log.info("Feature selection algorithm: MRMR");
		
		Path tempMax = null;
		for (int i=0; i<featureNumber; i++) {
			
			log.info("Generating candidates at stage " + i + "th");
			
			Path tempFeature = new Path(featureJobTempUri(temp, i));
			
			Configuration confFeature = getConf();
			confFeature.set(DefaultOptionCreator.TARGET_INDEX, ""+(targetColumn-1));
			confFeature.set(DefaultOptionCreator.COLUMN_NUMBER, ""+columnNumber);
			confFeature.set(DefaultOptionCreator.ROW_NUMBER, ""+rowNumber);
			
			Job jobFeature = HadoopUtil.prepareJob(input,
                           tempFeature,
                           TextInputFormat.class,
                           MRMRMapper.class,
                           IntWritable.class,
                           Text.class,
                           MRMRReducer.class,
                           LongWritable.class,
                           Text.class,
                           SequenceFileOutputFormat.class,
                           confFeature);
      jobFeature.setJobName("Feature Candidate stage " + i + "th");
      
			if (i>0) {
				DistributedCache.addCacheFile(new Path(cacheFileUri(temp, i)).toUri(), jobFeature.getConfiguration());
			}
                           
      boolean succeededFeature = jobFeature.waitForCompletion(true);
			if (!succeededFeature) return -1;
		
			// Selecting best candidate job
			log.info("Selecting the best candidate at stage " + i + "th");
			
			Configuration confMax = getConf();
			confFeature.set(DefaultOptionCreator.COLUMN_NUMBER, ""+columnNumber);
			
			tempMax = new Path(maxJobTempUri(temp, i));
			if (i == featureNumber-1) tempMax = outputPath;
			
			Job jobMax = HadoopUtil.prepareJob(tempFeature,
                           tempMax,
                           SequenceFileInputFormat.class,
                           Mapper.class,
                           LongWritable.class,
                           Text.class,
                           MaxReducer.class,
                           LongWritable.class,
                           Text.class,
                           TextOutputFormat.class,
                           confMax);
      jobMax.setJobName("Best candidate at stage " + i + "th");
			jobMax.setCombinerClass(MaxCombiner.class);
			
			if (i>0) {
				DistributedCache.addCacheFile(new Path(cacheFileUri(temp, i)).toUri(), jobMax.getConfiguration());
			}
                           
      boolean succeededMax = jobMax.waitForCompletion(true);
			if (!succeededMax) return -1;
			
			try {
				FileSystem hdfs = FileSystem.get(confMax);
				hdfs.delete(tempFeature, true);
			} catch (IOException e) {}
			
		}
		return 0;
	}
}
