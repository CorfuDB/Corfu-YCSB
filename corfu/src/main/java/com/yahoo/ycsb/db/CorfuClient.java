package com.yahoo.ycsb.db;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import org.corfudb.runtime.CorfuRuntime;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;


/**
 * Created by rmichoud on 12/1/16.
 *
 * CorfuDB doesn't have a concept of key/Values nor table. We will
 * use the following mapping for the Corfu Driver:
 *      - Table : SMRMAP backed by Stream (each table has it's "own log")
 *      - Key : Key in the SMRMAP
 *      - Value : Normal Map<String, String> with
 *          * Record fields = key
 *          * Record value = String
 *
 */
public class CorfuClient extends DB {
  private CorfuRuntime runtime;
  private Map<String, Map<String, String>> localCache;

  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.OFF);
    runtime = new CorfuRuntime()
        // Get config string as argument
        .parseConfigurationString("localhost:9000")
        .connect();

    /* Create default: usertable table */
    localCache = CorfuUtils.<String, Map<String, String>>createSMRMap("usertable", runtime);
  }

  /**
   * Read a record from the database.
   *
   * Each field/value pair from the result will be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return The result of the operation.
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
                     HashMap<String, ByteIterator> result) {
    /* Get a map backed by a stream that represent a table */
    Map<String, String> record = localCache.get(key);

    return CorfuUtils.readRecordFromStringMap(record, result, fields);

  }

  /**
   * Perform a range scan for a set of records in the database.
   *
   * Each field/value pair from the result will be
   * stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
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
    Map<String, String> record = localCache.get(key);

    return CorfuUtils.updateRecordInStringMap(record, values, runtime);

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
    Map<String, String> record = new HashMap<String, String>();

    Status st = CorfuUtils.populateRecordAsStringMap(record, values, runtime);

    localCache.put(key, record);

    return st;
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
    return null;
  }
}
