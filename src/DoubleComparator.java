import java.nio.ByteBuffer;
import org.apache.hadoop.io.WritableComparator;

/**
 * 
 * This class is used by the MapReduce framework to sort in descending order,
 * the Double key values resulted after running the Task2SortWikiStatsMapper.class   
 * A tipical example of pairs to be sorted: <1.3   aa>, <1.34   ab>, <1.19   ace>, <3.65  af>
 */
public class DoubleComparator extends WritableComparator {

  public DoubleComparator() {
    super();
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2,
        int s2, int l2) {
    Double v1 = ByteBuffer.wrap(b1, s1, l1).getDouble();
    Double v2 = ByteBuffer.wrap(b2, s2, l2).getDouble();
    return v1.compareTo(v2) * (-1);
  }
}
