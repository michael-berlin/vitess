package com.youtube.vitess.vtgate.integration.hadoop;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.youtube.vitess.vtgate.Exceptions.InvalidFieldException;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.hadoop.VitessInputFormat;
import com.youtube.vitess.vtgate.hadoop.writables.KeyspaceIdWritable;
import com.youtube.vitess.vtgate.hadoop.writables.RowWritable;
import com.youtube.vitess.vtgate.TestEnv;
import com.youtube.vitess.vtgate.integration.util.Util;
import com.youtube.vitess.vtgate.hadoop.utils.GsonAdapters;

import junit.extensions.TestSetup;
import junit.framework.TestSuite;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapReduceTestUtil;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration tests for MapReductions in Vitess. These tests use an in-process Hadoop cluster via
 * {@link HadoopTestCase}. These tests are JUnit3 style because of this dependency. Vitess setup for
 * these tests require at least one rdonly instance per shard.
 *
 */
public class MapReduceIT extends HadoopTestCase {

  public static TestEnv testEnv = getTestEnv();
  private static Path outputPath = new Path("/tmp");

  public MapReduceIT() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Util.createTable(testEnv);
  }

  /**
   * Run a mapper only MR job and verify all the rows in the source table were outputted into HDFS.
   */
  public void testDumpTableToHDFS() throws Exception {
    // Insert 20 rows per shard
    int rowsPerShard = 20;
    for (String shardName : testEnv.getShardKidMap().keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowsPerShard);
    }
    Util.waitForTablet("rdonly", 40, 3, testEnv);
    // Configurations for the job, output from mapper as Text
    Configuration conf = createJobConf();
    Job job = new Job(conf);
    job.setJobName("table");
    job.setJarByClass(VitessInputFormat.class);
    job.setMapperClass(TableMapper.class);
    VitessInputFormat.setInput(job,
        "localhost:" + testEnv.getPort(),
        testEnv.getKeyspace(),
        "select keyspace_id, name from vtgate_test",
        4,
        testEnv.getRpcClientFactory().getClass());
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(RowWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setNumReduceTasks(0);

    Path outDir = new Path(outputPath, "mrvitess/output");
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    FileOutputFormat.setOutputPath(job, outDir);

    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());

    String[] outputLines = MapReduceTestUtil.readOutput(outDir, conf).split("\n");
    // there should be one line per row in the source table
    assertEquals(rowsPerShard * 2, outputLines.length);
    Set<String> actualKids = new HashSet<>();
    Set<String> actualNames = new HashSet<>();

    // Rows are keyspace ids are written as JSON since this is
    // TextOutputFormat. Parse and verify we've gotten all the keyspace
    // ids and rows.
    Gson gson = new GsonBuilder()
        .registerTypeHierarchyAdapter(byte[].class, GsonAdapters.BYTE_ARRAY)
        .registerTypeAdapter(UnsignedLong.class, GsonAdapters.UNSIGNED_LONG)
        .registerTypeAdapter(Class.class, GsonAdapters.CLASS)
        .create();
    for (String line : outputLines) {
      String kidJson = line.split("\t")[0];
      Map<String, String> m = new HashMap<>();
      m = gson.fromJson(kidJson, m.getClass());
      actualKids.add(m.get("id"));

      String rowJson = line.split("\t")[1];
      Map<String, Map<String, Map<String, String>>> map = new HashMap<>();
      map = gson.fromJson(rowJson, map.getClass());
      actualNames.add(
          new String(Base64.decodeBase64(map.get("contents").get("name").get("value"))));
    }

    Set<String> expectedKids = new HashSet<>();
    for (KeyspaceId kid : testEnv.getAllKeyspaceIds()) {
      expectedKids.add(((UnsignedLong) kid.getId()).toString());
    }
    assertEquals(expectedKids.size(), actualKids.size());
    assertTrue(actualKids.containsAll(expectedKids));

    Set<String> expectedNames = new HashSet<>();
    for (int i = 1; i <= rowsPerShard; i++) {
      expectedNames.add("name_" + i);
    }

    assertEquals(rowsPerShard, actualNames.size());
    assertTrue(actualNames.containsAll(expectedNames));
  }

  /**
   * Map all rows and aggregate by keyspace id at the reducer.
   */
  public void testReducerAggregateRows() throws Exception {
    int rowsPerShard = 20;
    for (String shardName : testEnv.getShardKidMap().keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowsPerShard);
    }
    Util.waitForTablet("rdonly", 40, 3, testEnv);
    Configuration conf = createJobConf();

    Job job = new Job(conf);
    job.setJobName("table");
    job.setJarByClass(VitessInputFormat.class);
    job.setMapperClass(TableMapper.class);
    VitessInputFormat.setInput(job,
        "localhost:" + testEnv.getPort(),
        testEnv.getKeyspace(),
        "select keyspace_id, name from vtgate_test",
        1,
        testEnv.getRpcClientFactory().getClass());

    job.setMapOutputKeyClass(KeyspaceIdWritable.class);
    job.setMapOutputValueClass(RowWritable.class);

    job.setReducerClass(CountReducer.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(LongWritable.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    Path outDir = new Path(outputPath, "mrvitess/output");
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    FileOutputFormat.setOutputPath(job, outDir);

    job.waitForCompletion(true);
    assertTrue(job.isSuccessful());

    String[] outputLines = MapReduceTestUtil.readOutput(outDir, conf).split("\n");
    assertEquals(6, outputLines.length);
    int totalRowsReduced = 0;
    for (String line : outputLines) {
      totalRowsReduced += Integer.parseInt(line);
    }
    assertEquals(rowsPerShard * 2, totalRowsReduced);
  }

  public static class TableMapper extends
      Mapper<NullWritable, RowWritable, KeyspaceIdWritable, RowWritable> {
    @Override
    public void map(NullWritable key, RowWritable value, Context context) throws IOException,
        InterruptedException {
      try {
        KeyspaceId id = new KeyspaceId();
        id.setId(value.getRow().getULong("keyspace_id"));
        context.write(new KeyspaceIdWritable(id), value);
      } catch (InvalidFieldException e) {
        throw new IOException(e);
      }
    }
  }

  public static class CountReducer extends
      Reducer<KeyspaceIdWritable, RowWritable, NullWritable, LongWritable> {
    @Override
    public void reduce(KeyspaceIdWritable key, Iterable<RowWritable> values, Context context)
        throws IOException, InterruptedException {
      long count = 0;
      Iterator<RowWritable> iter = values.iterator();
      while (iter.hasNext()) {
        count++;
        iter.next();
      }
      context.write(NullWritable.get(), new LongWritable(count));
    }
  }

  /**
   * Create env with two shards each having a master and rdonly
   */
  static TestEnv getTestEnv() {
    Map<String, List<String>> shardKidMap = new HashMap<>();
    shardKidMap.put("-80",
        Lists.newArrayList("527875958493693904", "626750931627689502", "345387386794260318"));
    shardKidMap.put("80-",
        Lists.newArrayList("9767889778372766922", "9742070682920810358", "10296850775085416642"));
    TestEnv env = Util.getTestEnv(shardKidMap, "test_keyspace");
    env.addTablet("rdonly", 1);
    return env;
  }

  public static TestSetup suite() {
    return new TestSetup(new TestSuite(MapReduceIT.class)) {

      @Override
      protected void setUp() throws Exception {
        Util.setupTestEnv(testEnv);
      }

      @Override
      protected void tearDown() throws Exception {
        Util.teardownTestEnv(testEnv);
      }
    };

  }
}
