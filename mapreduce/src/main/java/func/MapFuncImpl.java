package func;

import common.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapFuncImpl implements MapFunc{
    @Override
    public List<Tuple2> doMap(String key, String value) {
        // 从文章中分割出单词
        Pattern pattern = Pattern.compile("[a-zA-Z]+");
        Matcher matcher = pattern.matcher(value);
        List<String> words = new ArrayList<>();
        while (matcher.find()) {
            words.add(matcher.group());
        }
        List<Tuple2> result = new ArrayList<>();
        // 转换为(key,1)
        for (String word : words) {
            Tuple2 kv = new Tuple2(word, "1");
            result.add(kv);
        }
        return result;
    }
}
