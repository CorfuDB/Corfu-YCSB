package com.yahoo.ycsb.db;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.yahoo.ycsb.*;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.StreamView;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

//import org.corfudb.protocols.wireprotocol.ILogUnitEntry;
import org.slf4j.LoggerFactory;


/**
 * Created by mwei on 1/25/16.
 */
public class CorfuClient extends DB {

  private final int numOfStreams = 100_000;

  private CorfuRuntime runtime;
  private ConcurrentHashMap<String, Map<String, HashMap<String, byte[]>>> objectCache;
  /**
   * Initialize any state for this DB.
   * Called once per DB instance; there is one DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    super.init();
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//  root.setLevel(Level.TRACE);
    root.setLevel(Level.OFF);
    runtime = new CorfuRuntime()
//              .parseConfigurationString("wilbur92.corp.gq1.yahoo.com:9000")
      .parseConfigurationString("localhost:9000")
      .connect();
    runtime.setCacheDisabled(true);

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

  private UUID getUUIDfromString(String id) {
    if (id == null) {
      return null;
    }
    try {
      return UUID.fromString(id);
    } catch (IllegalArgumentException iae) {
      UUID o = UUID.nameUUIDFromBytes(id.getBytes());
      return o;
    }
  }

  private Set<UUID> streamsFromString(String streamString)  {
    if (streamString == null) {
      return Collections.emptySet();
    }

    return Pattern.compile(",")
      .splitAsStream(streamString)
      .map(String::trim)
      .map(this::getUUIDfromString)
      .collect(Collectors.toSet());
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
//    Map<String, byte[]> records = getTable(table).get(key);
//    for (String s : fields) {
//      result.put(s, new ByteArrayByteIterator(records.get(s)));
//    }

    long intKey = Long.parseLong(key.replaceFirst("user", "")) % numOfStreams;
    StreamView s = runtime.getStreamsView().get(getUUIDfromString(Long.toString(intKey)));
    for (int i=0; true; ++i) {
      LogData r = s.read();
      if (r == null) {
//        System.out.println(i);
//        System.out.flush();
        return Status.OK;
      }
//      else {
//        ByteBuf buf = new ByteBuf();
//        r.getBuffer().getBytes(0, buf, r.getBuffer().readableBytes());
//        System.out.println("Reading: " + (new String(r.getBuffer().array())));
//        System.out.flush();
//        if (r.getBuffer().hasArray()) {
//          System.out.println("Array exists");
//          System.out.flush();
//          result.put(Integer.toString(i), new ByteArrayByteIterator(r.getBuffer().array()));
//        }
//      }
    }
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
    Map.Entry<String, ByteIterator> entry=values.entrySet().iterator().next();
    ByteIterator value=entry.getValue();

    long intKey = Long.parseLong(key.replaceFirst("user", "")) % numOfStreams;
    runtime.getStreamsView().write(streamsFromString(Long.toString(intKey)),
        (value.toArray()));

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
    Map.Entry<String, ByteIterator> entry=values.entrySet().iterator().next();
    ByteIterator value=entry.getValue();

    long intKey = Long.parseLong(key.replaceFirst("user", "")) % numOfStreams;
    byte[] val = value.toArray();
    runtime.getStreamsView().write(streamsFromString(Long.toString(intKey)),
        (val));

//    System.out.println("Size of value: " + val.length);
//    System.out.flush();
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