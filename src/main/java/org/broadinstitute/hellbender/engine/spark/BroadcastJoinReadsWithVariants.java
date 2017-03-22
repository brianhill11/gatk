package org.broadinstitute.hellbender.engine.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.internal.Logging;
import org.apache.spark.rdd.RDD;

import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipList;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipListOneContig;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.variant.GATKVariant;
import scala.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.spark.blaze.*;
import scala.Array;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassManifestFactory;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

/**
 * Joins an RDD of GATKReads to variant data using a broadcast strategy.
 *
 * The variants RDD is materialized as a List then broadcast using Spark's Broadcast variable mechanism.  The reads are
 * then mapped over and overlapping variants are added for each read.
 */
public final class BroadcastJoinReadsWithVariants {
    private BroadcastJoinReadsWithVariants(){}

    public static JavaPairRDD<GATKRead, Iterable<GATKVariant>> join(final JavaRDD<GATKRead> reads, final JavaRDD<GATKVariant> variants ) {
        final JavaSparkContext ctx = new JavaSparkContext(reads.context());
        final IntervalsSkipList<GATKVariant> variantSkipList = new IntervalsSkipList<>(variants.collect());
        final Broadcast<IntervalsSkipList<GATKVariant>> variantsBroadcast = ctx.broadcast(variantSkipList);

        // get Blaze
        final SparkContext sparkCtx = reads.context();
        final BlazeRuntime accel = new BlazeRuntime(sparkCtx);
        //
        // Create Broadcast data to be cached on accelerator
        //
        // start/end pairs for each IntervalSkipListOneContig object, concatenated together
        final List<Integer> variant_start_end = new ArrayList<>();
        // reach array for each bucket in each IntervalSkipListOneContig object, concatenated together
        final List<Integer> reach = new ArrayList<>();
        // length of each reach array for each IntervalSkipListOneContig object
        final List<Integer> reachLength = new ArrayList<>();
        // shift values for each IntervalSkipListOneContig
        final List<Integer> shift = new ArrayList<>();
        // number of intervals in each IntervalSkipListOneContig object
        final List<Integer> vs_size = new ArrayList<>();

        int i = 0;
        final Map<String, Integer> contig_index_map = new HashMap<>();
        // for each Contig in InvervalSkipList
        for (String key : variantSkipList.intervals.keySet()) {
            final IntervalsSkipListOneContig<GATKVariant> contig_intervals = variantSkipList.intervals.get(key);

            System.err.println("Contig: " + contig_intervals.contig);
            // for each sorted start/end pair in IntervalSkipListOneContig
            for (int j = 0; j < contig_intervals.vs.size(); j++) {
                final GATKVariant contig_interval = contig_intervals.vs.get(j);
                final Integer contig_interval_start = contig_interval.getStart();
                final Integer contig_interval_end = contig_interval.getEnd();
                variant_start_end.add(contig_interval_start);
                variant_start_end.add(contig_interval_end);
            }


            if (contig_intervals.reach.length != contig_intervals.reachLength) {
                System.err.println("WARNING: contig_intervals.reach.length != contig_intervals.reachLength");
                System.err.println("contig_interavls.reach.length:" + contig_intervals.reach.length + "\t contig_intervals.reachLength:" + contig_intervals.reachLength);
            }
            if (contig_intervals.reach.length > 0) {
                System.err.println("contig_interavls.reach.length: " + contig_intervals.reach.length);
                for (int j = 0; j < contig_intervals.reach.length; j++) {
                    reach.add(contig_intervals.reach[j]);
                }
            } else {
                System.err.println("ERROR: contig_intervals.reach.length is zero!");
            }
            reachLength.add(contig_intervals.reachLength);
            shift.add(contig_intervals.shift);
            vs_size.add(contig_intervals.vs.size());

            // create contig -> index mapping
            contig_index_map.put(key, i);
            i = i + 1;
        }

        System.err.println("Dumping IntervalSkipList data...");
        try {
            PrintWriter writer = new PrintWriter("/tmp/hill/intervalSkipList_dump.txt", "UTF-8");
            writer.println("variant_start_end");
            writer.println(variant_start_end.toString());
            writer.println("reach");
            writer.println(reach.toString());
            writer.println("reachLength");
            writer.println(reachLength.toString());
            writer.println("shift");
            writer.println(shift.toString());
            writer.println("vs_size");
            writer.println(vs_size.toString());

            writer.close();
        } catch (IOException e) {
        }

        // wrap all Broadcast variables with
        final ClassTag<Object[]> integer_list_classtag = ClassTag$.MODULE$.apply(variant_start_end.toArray().getClass());

        final BlazeBroadcast<Object[]> variant_start_end_bc = accel.wrap(ctx.broadcast(variant_start_end.toArray()), integer_list_classtag);
        final BlazeBroadcast<Object[]> reach_bc = accel.wrap(ctx.broadcast(reach.toArray()), integer_list_classtag);
        final BlazeBroadcast<Object[]> reachLength_bc = accel.wrap(ctx.broadcast(reachLength.toArray()), integer_list_classtag);
        final BlazeBroadcast<Object[]> shift_bc = accel.wrap(ctx.broadcast(shift.toArray()), integer_list_classtag);
        final BlazeBroadcast<Object[]> vs_size_bc = accel.wrap(ctx.broadcast(vs_size.toArray()), integer_list_classtag);

        final ArrayList<Integer> init_query_array = new ArrayList<>();

        //
        // Create Int array for input data
        //
        final JavaPairRDD<Integer, Tuple2<Integer, Integer>> start_end_list = reads.mapToPair(r -> {
            if (SimpleInterval.isValid(r.getContig(), r.getStart(), r.getEnd())) {
                return new Tuple2<>(contig_index_map.get(r.getContig()), new Tuple2<Integer, Integer>(r.getStart(), r.getEnd()));
            } else {
                //Sometimes we have reads that do not form valid intervals (reads that do not consume any ref bases, eg CIGAR 61S90I
                //In those cases, we'll just say that nothing overlaps the read
                return null;
            }
        });

        start_end_list.cache();
        System.err.println("start_end_list count:" + start_end_list.count());
        System.err.println("start_end_list num_partitions:" + start_end_list.getNumPartitions());

        Function2<ArrayList<Integer>, Tuple2<Integer, Integer>, ArrayList<Integer>> combine_pos = new Function2<ArrayList<Integer>, Tuple2<Integer, Integer>, ArrayList<Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public ArrayList<Integer> call(ArrayList<Integer> in1, Tuple2<Integer, Integer> in2) {
                // add start position
                in1.add(in2._1());
                // add end position
                in1.add(in2._2());
                return in1;
            }
        };

        Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>> merge_pos_arrays = new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
            private static final long serialVersionUID = 1L;
            @Override
            public ArrayList<Integer> call(ArrayList<Integer> in1, ArrayList<Integer> in2) {
                in1.addAll(in2);
                return in1;
            }
        };

        // group by Contig
        JavaPairRDD<Integer, ArrayList<Integer>> contig_query_arrays = start_end_list.aggregateByKey(
                init_query_array,
                combine_pos,
                merge_pos_arrays
        );

        contig_query_arrays.cache();
        System.err.println("contig_query_arrays count:" + contig_query_arrays.count());
        System.err.println("contig_query_arrays num_partitions:" + contig_query_arrays.getNumPartitions());
        contig_query_arrays.unpersist();
        start_end_list.unpersist();

        // for each contig, create batches of queries where the first value in the batch array
        // is the contig value, and then queries follow in start/end alternating order
        // ex: [contig_id, start1, end1, start2, end2, start3, end3, ..., startN, endN]
        // where N is the batch size
//        RDD<Integer[]> packed_query_arrays = contig_query_arrays.flatMap(k -> {
//            // batch size : number of query pairs in each array
//            final int batch_size = 100;
//            //
//            final Integer contig_id = k._1();
//            final ArrayList<Integer> queries = k._2();
//
//            final int num_batches = (int)Math.ceil(queries.size() / batch_size);
//            Integer[][] query_batches = new Integer[num_batches][batch_size + 1];
//            for (int b = 0; b < num_batches; b++) {
//                // for each batch, create new array to hold batch data
//                Integer[] query_batch = new Integer[batch_size + 1];
//                // add contig_id to head of list
//                query_batch[0] = contig_id;
//                // grab batch_size elements and add to query_batch
//                for (int q = 1; q <= batch_size; q++) {
//                    query_batch[q] = queries.get(b*batch_size + q-1);
//                }
//                //query_batch.addAll(queries.subList(b*batch_size, (b+1)*batch_size));
//                // add batch to list of batches
//                //query_batches.add(query_batch);
//                query_batches[b] = query_batch;
//            }
//            return query_batches;
//        });






//       final List<Integer> b = accel.wrap(packed_query_arrays).map_acc(new GetOverlappingAcc(
//                variant_start_end_bc,
//                reach_bc,
//                reachLength_bc,
//                shift_bc,
//                vs_size_bc)
//        ).collect();


//        System.err.println("packed_query_arrays:");
//        System.err.println(packed_query_arrays.collect());
//        System.err.println("num packed_query_arrays:" + packed_query_arrays.count());
        //System.err.println("query_array countByKey:" + contig_query_arrays.countByKey());
        //System.err.println("result:");
        //System.err.println(b);



            final JavaPairRDD<GATKRead, Iterable<GATKVariant>> t = reads.mapToPair(r -> {
                final IntervalsSkipList<GATKVariant> intervalsSkipList = variantsBroadcast.getValue();
                if (SimpleInterval.isValid(r.getContig(), r.getStart(), r.getEnd())) {
                    return new Tuple2<>(r, intervalsSkipList.getOverlapping(new SimpleInterval(r)));
                } else {
                    //Sometimes we have reads that do not form valid intervals (reads that do not consume any ref bases, eg CIGAR 61S90I
                    //In those cases, we'll just say that nothing overlaps the read
                    return new Tuple2<>(r, Collections.emptyList());
                }
            });

        System.err.println("Dumping Overlap data...");
        try {
            PrintWriter writer = new PrintWriter("/tmp/hill/overlapping_dump.txt", "UTF-8");

            t.collect().forEach(r -> {
                String var_intervals = "";
                Iterator<GATKVariant> iter = r._2().iterator();
                while (iter.hasNext()) {
                    GATKVariant variant = iter.next();
                    var_intervals += variant.getStart() + "," + variant.getEnd() + "|";
                }
                writer.println(r._1().getContig() + "," + r._1().getStart() + "," + r._1().getEnd() + ";" + var_intervals);
            });

            writer.close();
        } catch (IOException e) {
        }

        return reads.mapToPair(r -> {
            final IntervalsSkipList<GATKVariant> intervalsSkipList = variantsBroadcast.getValue();
            if (SimpleInterval.isValid(r.getContig(), r.getStart(), r.getEnd())) {
                final List<GATKVariant> isl = intervalsSkipList.getOverlapping(new SimpleInterval(r));

                return new Tuple2<>(r, intervalsSkipList.getOverlapping(new SimpleInterval(r)));
            } else {
                //Sometimes we have reads that do not form valid intervals (reads that do not consume any ref bases, eg CIGAR 61S90I
                //In those cases, we'll just say that nothing overlaps the read
                return new Tuple2<>(r, Collections.emptyList());
            }
        });
    }
}
