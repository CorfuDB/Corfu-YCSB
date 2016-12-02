package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by rmichoud on 12/1/16.
 */
public final class CorfuUtils {

  /**
   *  private so it cannot be instantiate.
   *  */
  private CorfuUtils() { }

  /**
   * Create a map backed by a Corfu SMRMAP.
   *
   * @param stream  Name of the stream backing the map
   * @param runtime Corfu runtime
   * @return A standard map backed by a SMRMAP
   */
  public static <T, V> Map<T, V> createSMRMap(String stream,
                                              CorfuRuntime runtime) {
    Map<T, V> map = runtime.getObjectsView()
        .build()
        .setStreamName(stream)
        .setType(SMRMap.class)
        .open();
    return map;
  }

  /**
   * Read from a Map<String, String> fields/values.
   *
   * The map represent a record with each key/value pair
   * represent field/values in this record. We are reading from it
   * and filling a result HashMap for YCSB api.
   *
   * @param record Map representing Fields/Values for a record
   * @param result HashMap to fill for YCSB api.
   * @param fields Fields to read from (if null, read from all)
   *
   * @return Status of the operation
   */
  public static Status readRecordFromStringMap(Map<String, String> record,
                                               HashMap<String, ByteIterator> result,
                                               Set<String> fields) {
    if (record == null) {
      return Status.NOT_FOUND;
    }
    if (fields != null) {
      for (String field: fields) {
        String val = record.get(field);
        if (val != null) {
          result.put(field, new StringByteIterator(val));
        } else {
          return Status.NOT_FOUND;
        }
      }
    } else {
      StringByteIterator.putAllAsByteIterators(result, record);
    }
    return Status.OK;

  }

  /**
   * Update fields of a record represented by a Map<String, String>.
   *
   * The map represent a record with each key/value pair
   * represent field/values in this record. The fields updated are passed
   * by the values parameter.
   *
   * @param record Map representing Fields/Values for a record
   * @param values Field/Values map to update in the record
   * @param runtime Corfu runtime to perform transaction
   *
   * @return Status of the operation
   */
  public static Status updateRecordInStringMap(Map<String, String> record,
                                               HashMap<String, ByteIterator> values,
                                               CorfuRuntime runtime) {
    if (record == null) {
      return Status.NOT_FOUND;
    }
    /* We use a Transaction because we want to update the map on batch mode */
    try {
      runtime.getObjectsView().TXBegin();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        record.put(entry.getKey(), entry.getValue().toString());
      }
      runtime.getObjectsView().TXEnd();
    } catch (TransactionAbortedException e) {
      return Status.ERROR;
    }

    return Status.OK;
  }

  public static Status populateRecordAsStringMap(Map<String, String> record,
                                               HashMap<String, ByteIterator> values,
                                               CorfuRuntime runtime){
    try {
            /* We use a Transaction because we want to update the map on batch mode */
      runtime.getObjectsView().TXBegin();
      for (Map.Entry<String, ByteIterator> val : values.entrySet()) {
        record.put(val.getKey(), val.getValue().toString());
      }
      runtime.getObjectsView().TXEnd();

    } catch (TransactionAbortedException e) {
      return Status.ERROR;
    }

    return Status.OK;
  }
}
