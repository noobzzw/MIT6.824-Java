package server;

import com.alibaba.fastjson.JSON;
import common.Cons;
import common.Task;
import common.Tuple2;
import func.MapFunc;
import func.MapFuncImpl;
import func.ReduceFunc;
import func.ReduceFuncImpl;
import rpc.io.RpcClient;
import util.FileUtil;
import util.LogUtil;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author razertory
 * @date 2021/1/1
 */
public class Worker {
    private final Integer masterHost;
    private Integer mapNum;
    private final Integer reduceNum;
    // 预设10个线程同时处理数据
    private final ExecutorService threadPool = Executors.newFixedThreadPool(10);

    public Worker(Integer masterHost) {
        this.masterHost = masterHost;
        this.reduceNum = 10;
        this.mapNum = 0;
    }

    public void work(MapFunc mapFunc, ReduceFunc reduceFunc) throws InterruptedException {
        final RpcClient rpcClient = new RpcClient();
        while (true) {
            // 不断的与master进行心跳
            Object o = rpcClient.call(masterHost, "assignTask");
            Task currentTask = JSON.parseObject(o.toString(), Task.class);
            if (currentTask.getType().equals(Cons.TASK_TYPE_MAP)) {
                doMap(currentTask, mapFunc::doMap);
//                threadPool.submit(() -> doMap(currentTask, mapFunc::doMap));
                mapNum++;
            } else if (currentTask.getType().equals(Cons.TASK_TYPE_REDUCE)) {
                doReduce(currentTask, reduceFunc::doReduce);
//                threadPool.submit(() ->doReduce(currentTask, reduceFunc::doReduce));
            } else if (currentTask.getType().equals(Cons.TASK_TYPE_ALL_DONE)) {
                LogUtil.log("Task Done, shutdown worker");
                return;
            } else {
//                LogUtil.log("Receive incorrect work!");
                Thread.sleep(2000);
            }
        }
    }

    /**
     * 执行用户传入的Map函数
     * @param task 当前task
     * @param mapFunction 用户编写的map function
     */
    private void doMap(Task task, BiFunction<String, String, List<Tuple2>> mapFunction) {
        /*
          intermediate：
          {
            {(word,1),(word,1)},
            {(char,1),(char,1)}
          }
         */
        List<List<Tuple2>> intermediate = new ArrayList<>();
        for (int i = 0; i < this.reduceNum; i++) {
            intermediate.add(new ArrayList<>());
        }
        String content = FileUtil.readFileToString(task.getTaskFile().getUrl());
        List<Tuple2> kvList = mapFunction.apply(task.getTaskFile().getFileName(), content);

        for (Tuple2 kv : kvList) {
            // partition
            intermediate.get(Math.abs(kv.getKey().hashCode()) % reduceNum).add(kv);
        }
        try {
            // 创建临时目录
            String tmpDir = "map_tmp";
            Files.createDirectories(Paths.get(tmpDir));
            for (int i = 0; i < reduceNum; i++) {
                if (intermediate.get(i).size() <= 0) {
                    continue;
                }
                // 创建临时文件
                final Path tmpFilePath = Paths.get(tmpDir, String.format("mr-tmp-%d-%d", task.getId(), i));
                Files.createFile(tmpFilePath);
                // append 进临时文件
                String template = "%s %s\n";
                FileWriter fw = new FileWriter(tmpFilePath.toFile(), true);
                // 将对应的map后的数据写入临时文件中（一个reduce对应一个临时文件）
                for (Tuple2 kv : intermediate.get(i)) {
                    fw.append(String.format(template, kv.getKey(), kv.getValue()));
                }
                fw.close();
            }
            // 编写完成，告知mater
            final String callResult = new RpcClient()
                    .call(Cons.MASTER_HOST, "doneMapTask", new Task[]{task})
                    .toString();
            if (!"success".equals(callResult)) {
                throw new Exception("map task finished, but master response failed");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doReduce(Task task, BiFunction<String, List<String>, Tuple2> reduceFunction) {
        // 创建结果文件
        Path filePath = Paths.get("reduce_result",String.format("reduce-%d",task.getId()));
        try {
            Files.createFile(filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // change file to (key,valueList)
        final List<Tuple2> tmpResult = new ArrayList<>();
        for (int i=0; i<mapNum; i++) {
            /*
              原url ： map_tmp/mr-tmp-#-1
              replace : map_tmp/mr-tmp-1-1
              第一位数字为map task序号
              第二位数字为reduce task序号
              每个reduce都读取第二位为自己id的所有文件
             */
            String fileUrl = task.getTaskFile()
                    .getUrl()
                    .replaceAll("#",String.valueOf(i));
            try {
                final List<String> lines = Files.readAllLines(Paths.get(fileUrl));
                // 先groupBy,获取key-valueList
                final Map<String, List<Tuple2>> collect = lines.stream()
                        .map(x -> new Tuple2(x.split(" ")[0], x.split(" ")[1]))
                        .collect(Collectors.groupingBy(Tuple2::getKey));
                // 在把value的类型从List<Tuple>转换为List<Value>
                for (Map.Entry<String, List<Tuple2>> keyValue : collect.entrySet()) {
                    final List<String> valueList = keyValue.getValue()
                            .stream()
                            .map(Tuple2::getValue)
                            .collect(Collectors.toList());
                    tmpResult.add(reduceFunction.apply(keyValue.getKey(), valueList));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } //end for
        // 进行总的reduce
        // list一直append，想着用链表更省空间
        List<Tuple2> result = new LinkedList<>();
        tmpResult.stream()
                .collect(Collectors.groupingBy(Tuple2::getKey))
                .forEach((k,v) -> {
                    final List<String> value = v.stream().map(Tuple2::getValue).collect(Collectors.toList());
                    result.add(reduceFunction.apply(k,value));
                });

        try (FileWriter fw = new FileWriter(filePath.toFile(), true);){
            for (Tuple2 kv : result) {
                fw.append(String.format("%s\t%s\n", kv.getKey(), kv.getValue()));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 编写完成，告知mater
        final String callResult = new RpcClient()
                .call(Cons.MASTER_HOST, "doneReduceTask", new Task[]{task})
                .toString();
        if (!"success".equals(callResult)) {
            LogUtil.log("reduce task finished, but master response failed");
        }

    }

    public static void main(String[] args) throws InterruptedException {
        final Worker worker = new Worker(Cons.MASTER_HOST);
        worker.work(new MapFuncImpl(), new ReduceFuncImpl());
    }

}
