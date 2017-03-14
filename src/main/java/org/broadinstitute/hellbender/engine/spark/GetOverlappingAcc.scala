
import scala.collection.mutable.ArrayBuffer;
import scala.util.control.Breaks.break;
import org.apache.spark.blaze.{Accelerator, BlazeBroadcast};

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

  override def getArgNum() = 5

  override def getArg(idx: Int): Option[_] = {
    if (idx == 0) Some(start_end)
    else if (idx == 1) Some(reach)
    else if (idx == 2) Some(reachLength)
    else if (idx == 3) Some(shift)
    else if (idx == 4) Some(vs_size)
    else None
  }
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
    def getOverlapping(query_contig: Int, query_start: Int, query_end: Int): Array[Int] = {

      // append start/end pairs to array
      val output_arr = ArrayBuffer.empty[Int]

      // use index to skip early non-overlapping entries.
      // NOTE: this gives you the index WITHIN THIS CONTIG
      var idx: Int = firstPotentiallyReaching(query_start, query_contig)
      if (idx < 0) {
        idx = 0
      }

      // need to add to index so that it falls within this contig
      // by adding farthest index of prev contig
      if (query_contig > 0) {
        idx += vs_size.data(query_contig - 1)
      }

      // [idx, contig_size) increment by 2 every time
      for (i <- idx until vs_size.data(query_contig) by 2 ) {
        val other_start: Int = start_end.data(i)
        val other_end: Int = start_end.data(i + 1)
        // they are sorted by start location, so if this one starts too late
        // then all of the others will, too.
        if (other_start > query_end) {
          break
        }
        if (overlaps(query_start, query_end, other_start, other_end)) {
          output_arr += other_start
          output_arr += other_end
        }
      }
      return output_arr.toArray
    }

    def overlaps(query_start: Int, query_end: Int, other_start: Int, other_end: Int): Boolean = {
      query_start <= other_end && other_start <= query_end
    }

    /* gets the index of first bin holding interval data
     *
     * @param position query starting position
     * @param contig_idx index of the contig for the query
     * @returns index of bin
     */
    def firstPotentiallyReaching(position: Int, contig_idx: Int): Int = {
      val contig_reach_length = reachLength.data(contig_idx)
      // [0, contig_reach_length)
      for (i <- 0 until contig_reach_length) {
        if (reach.data(contig_reach_length + i) >= position) {
          return i << shift.data(contig_idx)
        }
      }
      // no one reaches to the given position.
      return vs_size.data(contig_idx) - 1
    }
    return result_array.toArray
  }

}