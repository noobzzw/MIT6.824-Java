package func;

import common.Tuple2;

import java.util.List;

public class ReduceFuncImpl implements ReduceFunc{
    // word count
    @Override
    public Tuple2 doReduce(String key, List<String> valueList) {
        long sum = 0;
        for (String s : valueList) {
            sum += Integer.parseInt(s);
        }
        return new Tuple2(key,String.valueOf(sum));
    }
}
