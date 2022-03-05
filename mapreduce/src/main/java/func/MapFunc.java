package func;

import common.Tuple2;

import java.util.List;

public interface MapFunc {
    /**
     * map 函数
     * @param key 当前文件名
     * @param value 文件内容
     * @return (key,1) list
     */
    List<Tuple2> doMap(String key, String value);
}
