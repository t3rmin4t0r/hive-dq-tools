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

public class GenericUDAFByteDistEvaluator extends GenericUDAFBitsEvaluator {
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    BitCountBuffer bc = (BitCountBuffer) agg;
    ArrayList result = new ArrayList<List<LongWritable>>(Long.BYTES);
    for (int i = 0; i < Long.BYTES; i++) {
      ArrayList result1 = new ArrayList<LongWritable>(0xff);
      for (int j = 0; j < 0xff; j++) {
        result1.add(new LongWritable(bc.bytedist[i * 0xff + j]));
      }
      result.add(result1);
    }
    return result;
  }

  // list of lists
  @Override
  protected StandardListObjectInspector getFinalObjectInspector() {
    return (StandardListObjectInspector) ObjectInspectorFactory
        .getStandardListObjectInspector((StandardListObjectInspector) ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector));
  }
}
