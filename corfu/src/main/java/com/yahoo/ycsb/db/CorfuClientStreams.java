package com.yahoo.ycsb.db;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.yahoo.ycsb.*;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;


/**
 * Created by rmichoud on 11/30/16.
 *
 * CorfuDB doesn't have a concept of key/Values nor table. We will
 * use the following mapping for the Corfu Driver:
 *      - Table : Non-existent (All records are in the same one big table)
 *      - Key : Stream (Each "Tango object" has it's own stream)
 *      - Value : SMRMAP (State Machine Replication MAP, map backed by the log)
 *
 */
public class CorfuClientStreams extends DB {
  private CorfuRuntime runtime;

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
    Map<String, String> map = runtime.getObjectsView()
        .build()
        .setStreamName(key)
        .setType(SMRMap.class)
        .open();

    // System.out.println("Insert in table: " + table + " the key: " + key);

    try {
            /* We use a Transaction because we want to update the map on batch mode */
      runtime.getObjectsView().TXBegin();
      for (Map.Entry<String, ByteIterator> val : values.entrySet()) {
        map.put(val.getKey(), val.getValue().toString());
      }
      runtime.getObjectsView().TXEnd();

    } catch (TransactionAbortedException e) {
      return Status.ERROR;
    }

    return Status.OK;
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

      /* In order to read the map, we just need to instantiate it */
    Map<String, String> map = runtime.getObjectsView()
        .build()
        .setStreamName(key)
        .setType(SMRMap.class)
        .open();

    if (fields != null) {
      for (String field: fields) {
        String val = map.get(field);
        if (val != null) {
          result.put(field, new StringByteIterator(val));
        } else {
          return Status.NOT_FOUND;
        }
      }
    } else {
      StringByteIterator.putAllAsByteIterators(result, map);
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
    Map<String, String> map = runtime.getObjectsView()
        .build()
        .setStreamName(key)
        .setType(SMRMap.class)
        .open();

    /* We use a Transaction because we want to update the map on batch mode */
    try {
      runtime.getObjectsView().TXBegin();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        map.put(entry.getKey(), entry.getValue().toString());
      }
      runtime.getObjectsView().TXEnd();
    } catch (TransactionAbortedException e) {
      return Status.ERROR;
    }


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
    return Status.OK;
  }
}


