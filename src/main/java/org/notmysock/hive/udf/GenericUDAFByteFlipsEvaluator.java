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

public class GenericUDAFByteFlipsEvaluator extends GenericUDAFBitsEvaluator {
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    BitCountBuffer bc = (BitCountBuffer) agg;
    ArrayList result = new ArrayList<LongWritable>(Long.BYTES);
    for (int i = 0; i < Long.BYTES; i++) {
      result.add(new LongWritable(bc.byteflips[i]));
    }
    return result;
  }
}
