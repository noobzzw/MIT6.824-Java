package func;

import common.Tuple2;

import java.util.List;

public interface ReduceFunc {
    Tuple2 doReduce(String key, List<String> valueList);

}
