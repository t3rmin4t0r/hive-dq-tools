package org.notmysock.hive.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

public class GenericUDAFByteUniquesEvaluator extends GenericUDAFBitsEvaluator {
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    BitCountBuffer bc = (BitCountBuffer) agg;
    ArrayList result = new ArrayList<LongWritable>(Long.BYTES);
    for (int i = 0; i < Long.BYTES; i++) {
      int v = 0;
      for (int j = 0; j < 0xff; j++) {
        if (bc.bytedist[i * 0xff + j]!=0) {
          v++;
        }
      }
      result.add(new LongWritable(v));
    }
    return result;
  }
}
