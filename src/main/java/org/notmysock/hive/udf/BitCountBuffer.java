package org.notmysock.hive.udf;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AbstractAggregationBuffer;

public class BitCountBuffer extends AbstractAggregationBuffer implements Serializable {
  private static final long serialVersionUID = 1L;
  
  long[] bits = new long[Long.SIZE];
  long[] bitcount = new long[Long.SIZE];
  long[] bytecount = new long[Long.BYTES];
  long[] bytedist = new long[Long.BYTES*0xff];
  long[] bitflips = new long[Long.SIZE];
  long[] byteflips = new long[Long.BYTES];
  long[] bitruns = new long[Long.SIZE];
  long[] bit8runs = new long[Long.SIZE];
  long[] bitmaxruns = new long[Long.SIZE];
  long count = 0;
  transient byte[] prevbytes = new byte[Long.BYTES];
  transient long prevlong = 0;
  
  transient ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);

  public void add(long value) {
    count++;
    bitcount[Long.bitCount(value)]++;
    
    BitSet bs = BitSet.valueOf(new long[] { value });
    BitSet oldb = BitSet.valueOf(new long[] { prevlong });
    

    for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
      // operate on index i here
      bits[i]++;
    }
    
    byte[] bb = bbuf.putLong(0, value).array();
    
    for (int i = 0; i < bb.length; i++) {
      if (bb[i] != 0) {
        bytecount[i]++;
      }
      bytedist[i*0xff + (bb[i] & 0xff)]++;
    }

    for (int i = 0; i < prevbytes.length; i++) {
      if (bb[i] != prevbytes[i]) {
        byteflips[i]++;
      }
    }
    
    for (int i = 0; i < Long.SIZE; i++) {
      if (bitruns[i] == 8) {
        bit8runs[i]++;
      }
      if (oldb.get(i) != bs.get(i)) {
        bitflips[i]++;
        bitmaxruns[i] = Math.max(bitmaxruns[i], bitruns[i]);
        bitruns[i] = 0;
      } else {
        bitruns[i]++;
      }
    }

    System.arraycopy(bb, 0, prevbytes, 0, prevbytes.length);
    prevlong = value;
  }
  
  public void reset() {
    Arrays.fill(bits, 0);
    Arrays.fill(bitcount, 0);
    Arrays.fill(bytecount, 0);
    Arrays.fill(bytedist, 0);
    Arrays.fill(bitflips, 0);
    Arrays.fill(bitruns, 0);
    Arrays.fill(bit8runs, 0);
    Arrays.fill(bitmaxruns, 0);
    Arrays.fill(byteflips, 0);
    prevlong = 0;
    count = 0;
  }

  public void add(Double value) {
    add(Double.doubleToRawLongBits(value));
  }

  public void add(float value) {
    add(Float.floatToRawIntBits(value));
  }
  
  public void merge(BitCountBuffer bc) {
    for (int i = 0; i < bits.length; i++) {
      bits[i] += bc.bits[i];
      bitcount[i] += bc.bitcount[i];
      bitflips[i] += bc.bitflips[i];
      bitruns[i] += bc.bitruns[i];
      bit8runs[i] += bc.bitruns[i];
      bitmaxruns[i] = Math.max(bc.bitmaxruns[i], bitmaxruns[i]);
    }
    for (int i = 0; i < bytecount.length; i++) {
      bytecount[i] += bc.bytecount[i];
      byteflips[i] += bc.byteflips[i];
    }
    for (int i = 0; i < bytedist.length; i++) {
      bytedist[i] += bc.bytedist[i];
    }
    count += bc.count;
  }
}