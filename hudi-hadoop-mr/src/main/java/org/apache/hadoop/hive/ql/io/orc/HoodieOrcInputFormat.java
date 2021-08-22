/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StopWatch;
import org.apache.hadoop.util.StringUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.InputPathHandler;
import org.apache.hudi.hadoop.UseFileSplitsFromInputFormat;
import org.apache.hudi.hadoop.UseRecordReaderFromInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the Hoodie Mode. If paths
 * that does not correspond to a hoodie table then they are passed in as is (as what FileInputFormat.listStatus()
 * would do). The JobConf could have paths from multipe Hoodie/Non-Hoodie tables
 */
@UseRecordReaderFromInputFormat
@UseFileSplitsFromInputFormat
public class HoodieOrcInputFormat extends OrcInputFormat implements Configurable {

  private static final Logger LOG = LogManager.getLogger(HoodieOrcInputFormat.class);

//  public static final String INPUT_DIR_RECURSIVE = org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR_RECURSIVE;
//
//  private static final PathFilter HIDDEN_FILE_FILTER = p -> {
//    String name = p.getName();
//    return !name.startsWith("_") && !name.startsWith(".");
//  };

  private static final FileInputFormatHelper FILE_INPUT_FORMAT_HELPER = new FileInputFormatHelper();

  protected Configuration conf;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Configuration conf = job;
    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_ORC_MS_FOOTER_CACHE_ENABLED)) {
      // Create HiveConf once, since this is expensive.
      conf = new HiveConf(conf, OrcInputFormat.class);
    }

    Path[] paths = getInputPaths(conf);
    conf.set("mapred.input.dir", Arrays.stream(paths).map(Path::toString).collect(Collectors.joining(",")));
    List<OrcSplit> result = generateSplitsInfo(conf, new Context(conf, numSplits, createExternalCaches()));
    return result.toArray(new InputSplit[result.size()]);
  }

  static Path[] getInputPaths(Configuration conf) throws IOException {
    JobConf job = new JobConf(conf);
    List<Path> paths = new ArrayList<>();
    // Segregate inputPaths[] to incremental, snapshot and non hoodie paths
    List<String> incrementalTables = HoodieHiveUtils.getIncrementalTableNames(Job.getInstance(conf));
    InputPathHandler inputPathHandler = new InputPathHandler(conf, getInputPathsFromConf(conf), incrementalTables);

    Map<String, HoodieTableMetaClient> tableMetaClientMap = inputPathHandler.getTableMetaClientMap();
    // process incremental pulls first
    for (String table : incrementalTables) {
      HoodieTableMetaClient metaClient = tableMetaClientMap.get(table);
      if (metaClient == null) {
        /* This can happen when the INCREMENTAL mode is set for a table but there were no InputPaths
         * in the jobConf
         */
        continue;
      }
      List<Path> inputPaths = inputPathHandler.getGroupedIncrementalPaths().get(metaClient);
      List<FileStatus> result = listStatusForIncrementalMode(job, metaClient, inputPaths);
      if (result != null) {
        paths.addAll(result.stream().map(FileStatus::getPath).collect(Collectors.toList()));
      }
    }

    // process non hoodie Paths next.
    List<Path> nonHoodiePaths = inputPathHandler.getNonHoodieInputPaths();
    paths.addAll(nonHoodiePaths);

    // process snapshot queries next.
    List<Path> snapshotPaths = inputPathHandler.getSnapshotPaths();
    if (snapshotPaths.size() > 0) {
      List<FileStatus> result = HoodieInputFormatUtils.filterFileStatusForSnapshotMode(job, tableMetaClientMap, snapshotPaths);
      paths.addAll(result.stream().map(FileStatus::getPath).collect(Collectors.toList()));
    }

    LOG.info("Processing paths: " + paths);

    return paths.toArray(new Path[0]);
  }

  static Path[] getInputPathsFromConf(Configuration conf) throws IOException {
    String dirs = conf.get("mapred.input.dir");
    if (dirs == null) {
      throw new IOException("Configuration mapred.input.dir is not defined.");
    }
    String [] list = StringUtils.split(dirs);
    Path[] result = new Path[list.length];
    for (int i = 0; i < list.length; i++) {
      result[i] = new Path(StringUtils.unEscapeString(list[i]));
    }
    return result;
  }


  /**
   * Achieves listStatus functionality for an incrementally queried table. Instead of listing all
   * partitions and then filtering based on the commits of interest, this logic first extracts the
   * partitions touched by the desired commits and then lists only those partitions.
   */
  private static List<FileStatus> listStatusForIncrementalMode(JobConf job, HoodieTableMetaClient tableMetaClient, List<Path> inputPaths) throws IOException {
    String tableName = tableMetaClient.getTableConfig().getTableName();
    Job jobContext = Job.getInstance(job);
    Option<HoodieTimeline> timeline = HoodieInputFormatUtils.getFilteredCommitsTimeline(jobContext, tableMetaClient);
    if (!timeline.isPresent()) {
      return null;
    }
    Option<List<HoodieInstant>> commitsToCheck = HoodieInputFormatUtils.getCommitsForIncrementalQuery(jobContext, tableName, timeline.get());
    if (!commitsToCheck.isPresent()) {
      return null;
    }
    Option<String> incrementalInputPaths = HoodieInputFormatUtils.getAffectedPartitions(commitsToCheck.get(), tableMetaClient, timeline.get(), inputPaths);
    // Mutate the JobConf to set the input paths to only partitions touched by incremental pull.
    if (!incrementalInputPaths.isPresent()) {
      return null;
    }
    //List<Path> incremental = Arrays.stream(incrementalInputPaths.get().split(",")).map(Path::new).collect(Collectors.toList());
    //FileStatus[] fileStatuses = listStatus(job, incremental.toArray(new Path[0]));
    FileInputFormat.setInputPaths(job, incrementalInputPaths.get());
    FileStatus[] fileStatuses = FILE_INPUT_FORMAT_HELPER.listStatus(job);
    return HoodieInputFormatUtils.filterIncrementalFileStatus(jobContext, tableMetaClient, timeline.get(), fileStatuses, commitsToCheck.get());
  }

//  protected static FileStatus[] listStatus(JobConf job, Path[] dirs) throws IOException {
//    // get tokens for all the required FileSystems..
//    TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job);
//
//    // Whether we need to recursive look into the directory structure
//    boolean recursive = job.getBoolean(INPUT_DIR_RECURSIVE, false);
//
//    // creates a MultiPathFilter with the hiddenFileFilter and the
//    // user provided one (if any).
//    List<PathFilter> filters = new ArrayList<PathFilter>();
//    filters.add(HIDDEN_FILE_FILTER);
//    PathFilter jobFilter = getInputPathFilter(job);
//    if (jobFilter != null) {
//      filters.add(jobFilter);
//    }
//    PathFilter inputFilter = new MultiPathFilter(filters);
//
//    List<FileStatus> results = new ArrayList<>();
//    StopWatch sw = new StopWatch().start();
//
//    Iterable<FileStatus> locatedFiles = null;
//    try {
//      LocatedFileStatusFetcher locatedFileStatusFetcher = new LocatedFileStatusFetcher(job, dirs, recursive, inputFilter, false);
//      locatedFiles = locatedFileStatusFetcher.getFileStatuses();
//    } catch (InterruptedException e) {
//      throw new IOException("Interrupted while getting file statuses");
//    }
//    locatedFiles.forEach(results::add);
//
//    sw.stop();
//    if (LOG.isDebugEnabled()) {
//      LOG.debug("Time taken to get FileStatuses: " + sw.now(TimeUnit.MILLISECONDS));
//    }
//    LOG.info("Total input paths to process : " + results.size());
//    return results.toArray(new FileStatus[0]);
//  }
//
//  private static PathFilter getInputPathFilter(JobConf conf) {
//    Class<? extends PathFilter> filterClass = conf.getClass(
//            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.PATHFILTER_CLASS,
//            null, PathFilter.class);
//    return (filterClass != null) ? ReflectionUtils.newInstance(filterClass, conf) : null;
//  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

//  private static class MultiPathFilter implements PathFilter {
//    private List<PathFilter> filters;
//
//    public MultiPathFilter(List<PathFilter> filters) {
//      this.filters = filters;
//    }
//
//    public boolean accept(Path path) {
//      for (PathFilter filter : filters) {
//        if (!filter.accept(path)) {
//          return false;
//        }
//      }
//      return true;
//    }
//  }

  private static class FileInputFormatHelper extends FileInputFormat {

    @Override
    public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
      throw new HoodieException("getRecordReader for helper class should never be invoked");
    }

    public FileStatus[] listStatus(JobConf job) throws IOException {
      return super.listStatus(job);
    }

  }

}