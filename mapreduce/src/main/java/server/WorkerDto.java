package server;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * master方用于保存worker信息的类
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WorkerDto {
    private Integer workerId;
    // rpc 服务的host和ip
    private String host;
    private Integer port;
}
