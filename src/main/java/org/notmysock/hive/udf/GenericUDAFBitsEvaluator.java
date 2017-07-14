package org.notmysock.hive.udf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBinaryObjectInspector;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

public class GenericUDAFBitsEvaluator extends GenericUDAFEvaluator { 
  ObjectInspector inputOI;
  WritableBinaryObjectInspector partialOI;
  StandardListObjectInspector finalOI;

  ByteArrayOutputStream output = new ByteArrayOutputStream();

  /*
   * All modes returns BINARY columns.
   * 
   * PARTIAL1 takes in a primitive inspector
   * 
   * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator#init(org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode, org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector[])
   */
  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
    super.init(m, parameters);
    partialOI = PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
    finalOI = getFinalObjectInspector();
    switch (m) {
    case PARTIAL1: 
      inputOI = parameters[0];
    case PARTIAL2:
      return partialOI;
    case FINAL:
    case COMPLETE:
      return finalOI;
    default:
      throw new IllegalArgumentException("Unknown UDAF mode " + m);
    }
  }
  
  protected StandardListObjectInspector getFinalObjectInspector() {
    return (StandardListObjectInspector) ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
  }

  @Override
  public AbstractAggregationBuffer getNewAggregationBuffer()
      throws HiveException {
    return new BitCountBuffer();
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] args)
      throws HiveException {
    if (args[0] == null) {
      return;
    }
    
    BitCountBuffer b = ((BitCountBuffer)agg);
    Object val = ObjectInspectorUtils.copyToStandardJavaObject(args[0], inputOI);
    if (val instanceof Integer) {
      b.add(((Integer) val).intValue());
    } else if(val instanceof Long) {
      b.add(((Long) val).longValue());
    } else if (val instanceof Float) {
      b.add(((Float) val).floatValue());
    } else if (val instanceof Double) {
      b.add((Double)val);
    }
  }
  
  @Override
  public Object terminatePartial(AggregationBuffer agg) throws HiveException {
    BitCountBuffer bc = ((BitCountBuffer)agg);
    output.reset();
    try {
      ObjectOutputStream os = new ObjectOutputStream(output);
      os.writeObject(bc);
      os.flush();
      os.close();
    } catch(IOException ioe) {
      throw new HiveException(ioe);
    }
    return new BytesWritable(output.toByteArray());
  }
  
  @Override
  public void merge(AggregationBuffer agg, Object partial) throws HiveException {
    if (partial == null) {
      return;
    }
    final BytesWritable bw = partialOI.getPrimitiveWritableObject(partial);
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(bw.getBytes(), 0, bw.getLength());
      ObjectInputStream os = new ObjectInputStream(input);
      BitCountBuffer bc = (BitCountBuffer)os.readObject();
      ((BitCountBuffer)agg).merge(bc);
    } catch (Exception ioe) {
      throw new HiveException(ioe);
    }
  }
  
  @Override
  public void reset(AggregationBuffer agg) throws HiveException {
    ((BitCountBuffer)agg).reset();
  }
  
  @Override
  public Object terminate(AggregationBuffer agg) throws HiveException {
    BitCountBuffer bc = (BitCountBuffer)agg;
    ArrayList result = new ArrayList<LongWritable>(64);
    for (int i = 0; i < bc.bits.length; i++) {
      result.add(new LongWritable(bc.bits[i]));
    }
    return result;
  }
}