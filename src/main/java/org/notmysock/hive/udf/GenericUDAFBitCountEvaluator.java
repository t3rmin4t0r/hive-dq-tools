package org.notmysock.hive.udf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.io.LongWritable;

public class GenericUDAFBitCountEvaluator extends GenericUDAFBitsEvaluator {
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    BitCountBuffer bc = (BitCountBuffer)agg;
    ArrayList result = new ArrayList<LongWritable>(64);
    for (int i = 0; i < bc.bitcount.length; i++) {
      result.add(new LongWritable(bc.bitcount[i]));
    }
    return result;
  }

}
