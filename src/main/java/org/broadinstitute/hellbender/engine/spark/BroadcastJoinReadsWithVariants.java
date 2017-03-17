package org.broadinstitute.hellbender.engine.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.internal.Logging;

import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipList;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipListOneContig;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.variant.GATKVariant;
import scala.*;

import java.lang.reflect.*;
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

        // wrap all Broadcast variables with
/*        final ClassTag<List<Integer>> integer_list_classtag = ClassTag$.MODULE$.apply(List.class);
        final BlazeBroadcast variant_start_end_bc = accel.wrap(ctx.broadcast(variant_start_end), integer_list_classtag);
        final BlazeBroadcast reach_bc = accel.wrap(ctx.broadcast(reach), integer_list_classtag);
        final BlazeBroadcast reachLength_bc = accel.wrap(ctx.broadcast(reachLength), integer_list_classtag);
        final BlazeBroadcast shift_bc = accel.wrap(ctx.broadcast(shift), integer_list_classtag);
        final BlazeBroadcast vs_size_bc = accel.wrap(ctx.broadcast(vs_size), integer_list_classtag);*/

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

        Function2<ArrayList<Integer>, Tuple2<Integer, Integer>, ArrayList<Integer>> combine_pos = new Function2<ArrayList<Integer>, Tuple2<Integer, Integer>, ArrayList<Integer>>() {
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
            @Override
            public ArrayList<Integer> call(ArrayList<Integer> in1, ArrayList<Integer> in2) {
                in1.addAll(in2);
                return in1;
            }
        };
        
        JavaPairRDD<Integer, ArrayList<Integer>> query_array = start_end_list.aggregateByKey(
                init_query_array,
                combine_pos,
                merge_pos_arrays
        );

/*        final List<Integer> b = accel.wrap(query_array).map_acc(new GetOverlappingAcc(
                variant_start_end_bc,
                reach_bc,
                reachLength_bc,
                shift_bc,
                vs_size_bc)
        ).collect();*/

        System.err.println("query_array:");
        System.err.println(query_array);
        System.err.println("query_array countByKey:" + query_array.countByKey());
        System.err.println("result:");
        //System.err.println(b);


        return reads.mapToPair(r -> {
            final IntervalsSkipList<GATKVariant> intervalsSkipList = variantsBroadcast.getValue();
            if (SimpleInterval.isValid(r.getContig(), r.getStart(), r.getEnd())) {
                return new Tuple2<>(r, intervalsSkipList.getOverlapping(new SimpleInterval(r)));
            } else {
                //Sometimes we have reads that do not form valid intervals (reads that do not consume any ref bases, eg CIGAR 61S90I
                //In those cases, we'll just say that nothing overlaps the read
                return new Tuple2<>(r, Collections.emptyList());
            }
        });
    }
}
