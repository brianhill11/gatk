package org.broadinstitute.hellbender.engine.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipList;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipListOneContig;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.variant.GATKVariant;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

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
            final IntervalsSkipListOneContig contig_intervals = variantSkipList.intervals.get(key);

            // for each sorted start/end pair in IntervalSkipListOneContig
            for (int j = 0; j < contig_intervals.vs.size(); j++) {
                final SimpleInterval contig_interval = ((SimpleInterval)contig_intervals.vs.get(j));
                final Integer contig_interval_start = contig_interval.getStart();
                final Integer contig_interval_end = contig_interval.getEnd();
                variant_start_end.add(contig_interval_start);
                variant_start_end.add(contig_interval_end);
            }

            for (int j = 0; j < contig_intervals.reachLength; j++) {
                reach.add(contig_intervals.reach[i]);
            }
            reachLength.add(contig_intervals.reachLength);
            shift.add(contig_intervals.shift);
            vs_size.add(contig_intervals.vs.size());

            // create contig -> index mapping
            contig_index_map.put(key, i);
            i = i + 1;
        }

        // create hash set holding unique contig values, in order of insertion
//        final Set<String> contig_index_mapping_set = new Set<String>;
//        contig_index_mapping_set.addAll(reads.map(r -> {return r.getContig()}).collect());
//        final List<String> contig_index_mapping = new ArrayList<String>(contig_index_mapping_set);



        //
        // Create Int array for input data
        //
        final JavaRDD<Tuple3<Integer, Integer, Integer>> start_end_list = reads.map(r -> {
            if (SimpleInterval.isValid(r.getContig(), r.getStart(), r.getEnd())) {
                return new Tuple3<>(contig_index_map.get(r.getContig()), r.getStart(), r.getEnd());
            } else {
                //Sometimes we have reads that do not form valid intervals (reads that do not consume any ref bases, eg CIGAR 61S90I
                //In those cases, we'll just say that nothing overlaps the read
                return null;
            }
        });

        // input array for accelerator
        final List<Integer> contig_start_end = new ArrayList<>();
        // collect all of the query tuples
        final List<Tuple3<Integer, Integer, Integer>> query_list = start_end_list.collect();
        // for each query tuple, unpack and add to Int array
        for (Tuple3<Integer, Integer, Integer> query : query_list) {
            // add contig
            contig_start_end.add(query._1());
            // add query start location
            contig_start_end.add(query._2());
            // add query end location
            contig_start_end.add(query._3());
        }


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