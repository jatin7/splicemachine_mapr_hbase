package org.apache.hadoop.hbase.io.hfile;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DoNotPool;

/**
 * A Codec Pool designed for high concurrency usage.  The initial HBase approach had heavy synchronization coupled with class level hashing.  Yuck.  
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HBaseCodecPool {
  private static final ConcurrentMap<String, BlockingQueue<Compressor>> compressorPool = new ConcurrentHashMap<String, BlockingQueue<Compressor>>();
  private static final ConcurrentMap<String, BlockingQueue<Decompressor>> decompressorPool = new ConcurrentHashMap<String, BlockingQueue<Decompressor>>();
  static {
	  // setup resource pools when loaded
	  for (Algorithm algorithm: Compression.Algorithm.values()) {
		  ArrayBlockingQueue<Compressor> compressorQueue = new ArrayBlockingQueue<Compressor>(10000);
		  ArrayBlockingQueue<Decompressor> decompressorQueue= new ArrayBlockingQueue<Decompressor>(10000);
		  compressorPool.put(algorithm.getName(), compressorQueue);
		  decompressorPool.put(algorithm.getName(), decompressorQueue);
	  }	  
  }
  

  public static Compressor getCompressor(String name, CompressionCodec codec, Configuration conf) {
	  BlockingQueue<Compressor> compressorQueue = compressorPool.get(name);
	  Compressor compressor = compressorQueue.poll();
	  if (compressor != null) 
		  return compressor;
	  return codec.createCompressor();
  }
  
  public static Compressor getCompressor(String name, CompressionCodec codec) {
    return getCompressor(name, codec, null);
  }
  
  public static Decompressor getDecompressor(String name, CompressionCodec codec) {
	  BlockingQueue<Decompressor> decompressorQueue = decompressorPool.get(name);
	  Decompressor decompressor = decompressorQueue.poll();
	  if (decompressor != null) 
		  return decompressor;
	  return codec.createDecompressor();
  }
  
  public static void returnCompressor(String name, Compressor compressor) {
	    if (compressor == null) {
	      return;
	    }
	    // if the compressor can't be reused, don't pool it.
	    if (compressor.getClass().isAnnotationPresent(DoNotPool.class)) {
	      return;
	    }
	    compressor.reset();
	    compressorPool.get(name).offer(compressor);
  }
  

  public static void returnDecompressor(String name, Decompressor decompressor) {
	    if (decompressor == null) {
	        return;
	      }
	      // if the compressor can't be reused, don't pool it.
	      if (decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
	        return;
	      }
	      decompressor.reset();
	      decompressorPool.get(name).offer(decompressor);
  }
 
}
