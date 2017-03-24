package org.broadinstitute.hellbender.engine.spark

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
import org.apache.spark.blaze.{Accelerator, BlazeBroadcast}
import org.apache.spark.rdd.RDD
import org.broadinstitute.hellbender.utils.variant.GATKVariant

/*
 *
 * @param start_end: packed array of start/end pairs, (i.e. every
 * other value is a start or an end
 * @param reach: holds farthest interval value for each

 */

class GetOverlappingAcc(start_end: BlazeBroadcast[Array[Int]],
                        reach: BlazeBroadcast[Array[Int]],
                        reachLength: BlazeBroadcast[Array[Int]],
                        shift: BlazeBroadcast[Array[Int]],
                        vs_size: BlazeBroadcast[Array[Int]])
  extends Accelerator[Array[Int], Array[Int]] {

  // Accelerator ID string
  val id: String = "GetOverlapping"

  def getArgNum() = 5

  def getArg(idx: Int): Option[_] = if (idx == 0) Some(start_end.asInstanceOf[BlazeBroadcast[Array[Int]]])
  else if (idx == 1) Some(reach.asInstanceOf[BlazeBroadcast[Array[Int]]])
  else if (idx == 2) Some(reachLength.asInstanceOf[BlazeBroadcast[Array[Int]]])
  else if (idx == 3) Some(shift.asInstanceOf[BlazeBroadcast[Array[Int]]])
  else if (idx == 4) Some(vs_size.asInstanceOf[BlazeBroadcast[Array[Int]]])
  else None
  /* The input to the function is an array of tuples representing
   * (query_contig_idx, query_start, query_end)
   * so to iterate through the array you need to increment the index
   * by 3. The query_contig_idx is an integer value that matches the
   * order of the contigs in the contig mapping list.
   * The query_start and query_end values are the starting and ending
   * positions of the query interval (not array indexes)
   *
   * The output of the array is another integer array, where the order
   * of the array matches the order of the input array, and a value of
   * (-1) represents the boundary between the resulting arrays of
   * different queries. Here is pseudocode for parsing the output:
   *
   * final_result = []
   * result_num = 0
   * idx = 0
   * result_buffer = []
   *
   * while (idx > MAX_RESULT_ADDR && result_num < NUM_INPUTS)
   *    while (result_array[idx] >= 0)
   *        result_buffer.append(result_array[idx])
   *        idx += 1
   *    final_result[result_num].append(result_buffer)
   *    result_num += 1
   */
  override def call(in: Array[Int]) : Array[Int] = {

    println("Running Accel code on CPU with input array of size" + in.length)
    val num_inputs = in.length / 3
    val result_array = ArrayBuffer.empty[Int]
    // iterate over each query Tuple3
    for (i <- 0 until in.length by 3) {
      val query_contig = in(i)
      val query_start = in(i + 1)
      val query_end = in(i + 2)
      // append overlapping intervals to result array
      result_array ++ getOverlapping(query_contig, query_start, query_end)
      // append (-1) marker to signal end of results for this query
      result_array += -1
    }




    /*
     *
     * @param query_contig index of contig in mapping list
     * @param query_start start location of query interval
     * @param query_end end location of query interval
     * @returns result array containing start/end locations that overlap query
     */
    def getOverlapping(query_contig: Int, query_start: Int, query_end: Int) = {

      println("calling getOverlapping with contig:" + query_contig +  " start: " + query_start + " end: " + query_end)
      // append start/end pairs to array
      val output_arr = ArrayBuffer.empty[Int]

      // use index to skip early non-overlapping entries.
      // NOTE: this gives you the index WITHIN THIS CONTIG
      var idx = firstPotentiallyReaching(query_start, query_contig)
      if (idx < 0) idx = 0

      // need to add to index so that it falls within this contig
      // by adding farthest index of prev contig
      if (query_contig > 0) idx += vs_size.data(query_contig - 1).asInstanceOf[Int]

      // [idx, contig_size) increment by 2 every time
      for (i <- idx until vs_size.data(query_contig).asInstanceOf[Int] by 2 ) {
        val other_start = start_end.data(i).asInstanceOf[Int]
        val other_end = start_end.data(i + 1).asInstanceOf[Int]
        // they are sorted by start location, so if this one starts too late
        // then all of the others will, too.
        if (other_start > query_end) break
        if (overlaps(query_start, query_end, other_start, other_end)) {
          output_arr += other_start
          output_arr += other_end
        }
      }
      output_arr.toArray
    }

    def overlaps(query_start: Int, query_end: Int, other_start: Int, other_end: Int) = query_start <= other_end && other_start <= query_end

    /* gets the index of first bin holding interval data
     *
     * @param position query starting position
     * @param contig_idx index of the contig for the query
     * @returns index of bin
     */
    def firstPotentiallyReaching(position: Int, contig_idx: Int) : Int = {
      val contig_reach_length = reachLength.data(contig_idx).asInstanceOf[Int]
      // [0, contig_reach_length)
      for (i <- 0 until contig_reach_length) {
        if (reach.data(contig_reach_length + i).asInstanceOf[Int] >= position) return i << shift.data(contig_idx).asInstanceOf[Int]
      }
      // no one reaches to the given position.
      return vs_size.data(contig_idx).asInstanceOf[Int] - 1
    }
    result_array.toArray
  }

}

//object query_transformer {
//    def create_query_array_from_GATKVariant_RDD(reads: RDD[GATKVariant], contig_index_map: Map[String, Integer]) : Array[Integer] = {
//        val init_agg_array = ArrayBuffer.empty[Integer]
//        val read_contig_map : RDD[(Integer, (Integer, Integer))] = reads.map(v => (contig_index_map(v.getContig), (v.getStart.asInstanceOf[Integer], v.getEnd.asInstanceOf[Integer])))
//        // merge query positions in same contig (start, followed by end)
//        val add_to_array = (pos_per_contig: ArrayBuffer[Integer], pos: (Integer, Integer)) => {
//          pos_per_contig += pos._1
//          pos_per_contig += pos._2
//        }
//        // merge position arrays of same contig
//        val merge_contig_arrays = (a: ArrayBuffer[Integer], b: ArrayBuffer[Integer]) => a ++ b
//        // aggreate by key, key = contig
//        val aggregate_by_contig = read_contig_map.aggregateByKey(init_agg_array)(add_to_array, merge_contig_arrays)
//        println(aggregate_by_contig)
//    }
//}