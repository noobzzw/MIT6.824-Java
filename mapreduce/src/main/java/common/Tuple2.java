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
public class Tuple2 {
    private String key;
    private String value;
}
