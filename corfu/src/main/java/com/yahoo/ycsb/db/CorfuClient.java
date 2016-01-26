package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by mwei on 1/25/16.
 */
public class CorfuClient extends DB {

  private CorfuRuntime runtime;
  private ConcurrentHashMap<String, Map<String, HashMap<String, byte[]>>> objectCache;
  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    runtime = new CorfuRuntime()
              .parseConfigurationString("localhost:9000")
              .connect();
    objectCache = new ConcurrentHashMap<>();
  }

  @SuppressWarnings("unchecked")
  private Map<String, HashMap<String, byte[]>> getTable(String table) {
    Map<String, HashMap<String, byte[]>> out = objectCache.get(table);
    if (out == null){
      out = runtime.getObjectsView().open(table, SMRMap.class);
      objectCache.put(table, out);
    }
    return out;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    Map<String, byte[]> records = getTable(table).get(key);
    for (String s : fields) {
      result.put(s, new ByteArrayByteIterator(records.get(s)));
    }
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value pair from the result will be
   * stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return The result of the operation.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return null;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into
   * the record with the specified
   * record key, overwriting any existing values with the same field name.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return The result of the operation.
   */
  @Override
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    HashMap<String, byte[]> records = getTable(table).get(key);
    HashMap<String, byte[]> toInsert = new HashMap<>();
    for (String s : values.keySet()) {
      toInsert.put(key, values.get(s).toArray());
    }
    records.putAll(toInsert);
    // Forcefully update the map, since values is not an SMR object.
    getTable(table).put(key, records);
    return Status.OK;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into
   * the record with the specified
   * record key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return The result of the operation.
   */
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    HashMap<String, byte[]> toInsert = new HashMap<>();
    for (String s : values.keySet()) {
      toInsert.put(key, values.get(s).toArray());
    }
    getTable(table).put(key, toInsert);
    return Status.OK;
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return The result of the operation.
   */
  @Override
  public Status delete(String table, String key) {
    getTable(table).remove(key);
    return Status.OK;
  }
}
