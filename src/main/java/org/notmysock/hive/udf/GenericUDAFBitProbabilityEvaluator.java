package org.notmysock.hive.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class GenericUDAFBitProbabilityEvaluator extends GenericUDAFBitsEvaluator {
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    BitCountBuffer bc = (BitCountBuffer)agg;
    ArrayList result = new ArrayList<DoubleWritable>(64);
    for (int i = 0; i < bc.bits.length; i++) {
      Double b = (double) bc.count;
      result.add(new DoubleWritable(bc.bits[i]/b));
    }
    return result;
  }
  
  @Override
  protected StandardListObjectInspector getFinalObjectInspector() {
    return (StandardListObjectInspector) ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
  }
}
