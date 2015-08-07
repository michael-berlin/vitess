package com.youtube.vitess.vtgate.integration;

import com.google.common.collect.Iterables;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.BindVariable;
import com.youtube.vitess.vtgate.KeyRange;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.TestEnv;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.StreamCursor;
import com.youtube.vitess.vtgate.integration.util.Util;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.List;

/**
 * Test cases for streaming queries in VtGate
 *
 */
@RunWith(JUnit4.class)
public class StreamingVtGateIT {
  public static TestEnv testEnv = VtGateIT.getTestEnv();

  @BeforeClass
  public static void setUpVtGate() throws Exception {
    Util.setupTestEnv(testEnv);
  }

  @AfterClass
  public static void tearDownVtGate() throws Exception {
    Util.teardownTestEnv(testEnv);
  }

  @Before
  public void createTable() throws Exception {
    Util.createTable(testEnv);
  }

  @Test
  public void testStreamCursorType() throws Exception {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.getPort(), 0, testEnv.getRpcClientFactory());
    String selectSql = "select * from vtgate_test";
    Query allRowsQuery = new QueryBuilder(selectSql, testEnv.getKeyspace(), "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).setStreaming(true).build();
    Cursor cursor = vtgate.execute(allRowsQuery);
    Assert.assertEquals(StreamCursor.class, cursor.getClass());
    vtgate.close();
  }

  /**
   * Test StreamExecuteKeyspaceIds query on a single shard
   */
  @Test
  public void testStreamExecuteKeyspaceIds() throws Exception {
    int rowCount = 10;
    for (String shardName : testEnv.getShardKidMap().keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowCount);
    }
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.getPort(), 0, testEnv.getRpcClientFactory());
    for (String shardName : testEnv.getShardKidMap().keySet()) {
      String selectSql = "select A.* from vtgate_test A join vtgate_test B join vtgate_test C";
      Query query = new QueryBuilder(selectSql, testEnv.getKeyspace(), "master")
          .setKeyspaceIds(testEnv.getKeyspaceIds(shardName)).setStreaming(true).build();
      Cursor cursor = vtgate.execute(query);
      int count = Iterables.size(cursor);
      Assert.assertEquals((int) Math.pow(rowCount, 3), count);
    }
    vtgate.close();
  }

  /**
   * Same as testStreamExecuteKeyspaceIds but for StreamExecuteKeyRanges
   */
  @Test
  public void testStreamExecuteKeyRanges() throws Exception {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.getPort(), 0, testEnv.getRpcClientFactory());
    int rowCount = 10;
    for (String shardName : testEnv.getShardKidMap().keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowCount);
    }
    for (String shardName : testEnv.getShardKidMap().keySet()) {
      List<KeyspaceId> kids = testEnv.getKeyspaceIds(shardName);
      KeyRange kr = new KeyRange(Collections.min(kids), Collections.max(kids));
      String selectSql = "select A.* from vtgate_test A join vtgate_test B join vtgate_test C";
      Query query = new QueryBuilder(selectSql, testEnv.getKeyspace(), "master").addKeyRange(kr)
          .setStreaming(true).build();
      Cursor cursor = vtgate.execute(query);
      int count = Iterables.size(cursor);
      Assert.assertEquals((int) Math.pow(rowCount, 3), count);
    }
    vtgate.close();
  }

  /**
   * Test scatter streaming queries fetch rows from all shards
   */
  @Test
  public void testScatterStreamingQuery() throws Exception {
    int rowCount = 10;
    for (String shardName : testEnv.getShardKidMap().keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowCount);
    }
    String selectSql = "select A.* from vtgate_test A join vtgate_test B join vtgate_test C";
    Query query = new QueryBuilder(selectSql, testEnv.getKeyspace(), "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).setStreaming(true).build();
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.getPort(), 0, testEnv.getRpcClientFactory());
    Cursor cursor = vtgate.execute(query);
    int count = Iterables.size(cursor);
    Assert.assertEquals(2 * (int) Math.pow(rowCount, 3), count);
    vtgate.close();
  }

  @Test
  @Ignore("currently failing as vtgate doesn't set the error")
  public void testStreamingWrites() throws Exception {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.getPort(), 0, testEnv.getRpcClientFactory());

    vtgate.begin();
    String insertSql = "insert into vtgate_test " + "(id, name, age, percent, keyspace_id) "
        + "values (:id, :name, :age, :percent, :keyspace_id)";
    KeyspaceId kid = testEnv.getAllKeyspaceIds().get(0);
    Query query = new QueryBuilder(insertSql, testEnv.getKeyspace(), "master")
        .addBindVar(BindVariable.forULong("id", UnsignedLong.valueOf("" + 1)))
        .addBindVar(BindVariable.forString("name", ("name_" + 1)))
        .addBindVar(BindVariable.forULong("keyspace_id", (UnsignedLong) kid.getId()))
        .addKeyspaceId(kid)
        .setStreaming(true)
        .build();
    vtgate.execute(query);
    vtgate.commit();
    vtgate.close();
  }
}
