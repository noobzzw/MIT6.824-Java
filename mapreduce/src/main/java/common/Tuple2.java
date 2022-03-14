package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * key-value class
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Tuple2<K,V> {
    private K key;
    private V value;
}
