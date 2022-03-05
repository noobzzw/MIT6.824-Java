package server;/*
 * server.Master.java

 */

import com.alibaba.fastjson.JSON;
import common.Cons;
import common.MRDto;
import common.Task;
import common.TaskFile;
import rpc.io.RpcServer;
import util.FileUtil;
import util.LogUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author razertory
 * @date 2021/1/1
 * dispatch task to worker
 */
public class Master extends RpcServer {
    private final Integer reduceNum;
    private Boolean mapsDone;
    private Boolean done;
    // 文件地址
    private String dir;
    // 存储已就绪的map task
    private final Map<Integer, Task> mapTasksReady;
    // 存储正在运行的map task
    private final Map<Integer, Task> mapTasksInProgress;
    // reduce task, similar as above
    private final Map<Integer, Task> reduceTasksReady;
    private final Map<Integer, Task> reduceTasksInProgress;

    public Master() {
        String dir = "/Users/zhangziwen/gitproject/MIT6.824-Java/mapreduce/src/main/resources/articles";
        List<String> files = FileUtil.readDirFile(dir);
        mapTasksReady = new HashMap<>(16);
        mapTasksInProgress = new HashMap<>(16);
        reduceTasksReady = new HashMap<>(16);
        reduceTasksInProgress = new HashMap<>(16);
        for (int i = 0; i < Objects.requireNonNull(files).size(); i++) {
            final Task task = new Task(i, Cons.TASK_TYPE_MAP,
                    new TaskFile(Cons.TASK_STATUS_TODO, dir + "/" + files.get(i),files.get(i)),
                    System.currentTimeMillis());
            mapTasksReady.put(i,task);
        }
        mapsDone = false;
        done = false;
        reduceNum = 10;
    }

    public void start() {
        try {
            // start serve
            serve(Cons.MASTER_HOST);
        } catch (Exception e) {
            LogUtil.log("fail to start master server on port: " + Cons.MASTER_HOST);
        }
    }

    /**
     * 按照时间，将task放入就绪map中
     */
    private void collectStallTasks() {
        //collect when task overrun 1s
        long curTime = System.currentTimeMillis();
        Set<Map.Entry<Integer, Task>> mEntrySet = mapTasksInProgress.entrySet();
        Iterator<Map.Entry<Integer, Task>> mIterator = mEntrySet.iterator();
        while (mIterator.hasNext()) {
            Map.Entry<Integer, Task> entry = mIterator.next();
            if (curTime - entry.getValue().getTimestamp() > 10000) {
                mapTasksReady.put(entry.getKey(), entry.getValue());
                mIterator.remove();
                System.out.printf("Collect map task %d\n", entry.getKey());
            }
        }
        Set<Map.Entry<Integer, Task>> nEntrySet = reduceTasksInProgress.entrySet();
        Iterator<Map.Entry<Integer, Task>> nIterator = nEntrySet.iterator();
        while (nIterator.hasNext()) {
            Map.Entry<Integer, Task> entry = nIterator.next();
            if (curTime - entry.getValue().getTimestamp() > 10000) {
                reduceTasksReady.put(entry.getKey(), entry.getValue());
                nIterator.remove();
                System.out.printf("Collect reduce task %d\n", entry.getKey());
            }
        }
    }

    /**
     * 指派任务
     * 被worker远程调用
     * @return 要执行的task信息
     * todo 后续换为MRDto
     *
     */
    public String assignTask() {
        // 多个worker同时访问，使用synchronize
        // todo 这里用CurrentHashMap可能会更好一点
        synchronized (this) {
//            collectStallTasks();
            Task response = new Task();
            if (mapTasksReady.size() > 0) {
                Set<Map.Entry<Integer, Task>> entrySet = mapTasksReady.entrySet();
                for (Map.Entry<Integer, Task> entry : entrySet) {
                    entry.getValue().setTimestamp(System.currentTimeMillis());
                    response = entry.getValue();
                    mapTasksInProgress.put(entry.getKey(), entry.getValue());
                    mapTasksReady.remove(entry.getKey());
                    LogUtil.log("Distribute map task : " + response.getId());
                    return JSON.toJSONString(response);
                }
            } else if (mapTasksInProgress.size() > 0) {
                response.setType(Cons.TASK_TYPE_WAIT);
                return JSON.toJSONString(response);
            }
            mapsDone = true;

            // reduce
            if (reduceTasksReady.size() > 0) {
                Set<Map.Entry<Integer, Task>> entrySet = reduceTasksReady.entrySet();
                for (Map.Entry<Integer, Task> entry : entrySet) {
                    entry.getValue().setTimestamp(System.currentTimeMillis());
                    response = entry.getValue();
                    reduceTasksInProgress.put(entry.getKey(), entry.getValue());
                    reduceTasksReady.remove(entry.getKey());
                    LogUtil.log("Distribute reduce task: " + response.getId());
                    return JSON.toJSONString(response);
                }
            } else if (reduceTasksInProgress.size() > 0) {
                response.setType(Cons.TASK_TYPE_WAIT);
            } else {
                response.setType(Cons.TASK_TYPE_ALL_DONE);
            }
            done = true;
            return JSON.toJSONString(response);
        }
    }

    /**
     * worker 执行完map task rpc调用该方法报告
     * @param task 执行完成的task
     * @return master是否相应成功
     */
    public String doneMapTask(Task task) {
        String result = "success";
        try {
            mapTasksInProgress.remove(task.getId());
            // 只有ready和in process的所有任务都处理完了，才能算map task finish
            if (mapTasksInProgress.size() == 0 && mapTasksReady.size() == 0) {
                mapDone();
            }
        } catch (Exception e) {
            result = "fail";
        }
        return result;
    }


    /**
     * 当map task都完成时，由master调用该方法
     */
    private void mapDone() {
        LogUtil.log("Map task all done");
        mapsDone = true;
        // 开启reduce任务
        String mapTmpFileDir = "map_tmp";
        String fileNameFormat = "mr-tmp-#-%d";
        for (int i=0; i<reduceNum; i++) {
            // 生成reduce task
            final String filePath = Paths.get(mapTmpFileDir, String.format(fileNameFormat, i)).toString();
            // reduce 会读取多个file，这里fileName设为空
            final Task task = new Task(i, Cons.TASK_TYPE_REDUCE,
                    new TaskFile(Cons.TASK_STATUS_TODO, filePath,""),
                    System.currentTimeMillis());
            reduceTasksReady.put(i,task);
        }
        // 情况reduce result文件夹，防止覆盖
        Path resultDir = Paths.get("reduce_result");
        if (Files.isDirectory(resultDir)) {
            try {
                FileUtil.delete(resultDir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            // 删除后再创建
            Files.createDirectory(resultDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String doneReduceTask(Task task) {
        String result = "success";
        try {
            reduceTasksInProgress.remove(task.getId());
            // 只有ready和in process的所有任务都处理完了，才能算map task finish
            if (reduceTasksInProgress.size() == 0 && reduceTasksReady.size() == 0) {
                reduceDone();
            }
        } catch (Exception e) {
            result = "fail";
        }
        return result;
    }

    private void reduceDone() {
        done = true;
        LogUtil.log("Job Done!!!");
        // 清除临时文件
        try {
            FileUtil.delete(Paths.get("map_tmp"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        final Master master = new Master();
        master.start();
    }

}
