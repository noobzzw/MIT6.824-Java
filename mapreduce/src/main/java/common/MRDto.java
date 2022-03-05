package common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
/**
 *  DTO ： Data Transfer Object
 *  worker与master沟通时的message
 */
public class MRDto {
    private Integer taskType;
    private String jobFile;
}
