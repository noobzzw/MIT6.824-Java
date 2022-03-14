package server;/*
 * server.Master.java

 */

import common.Cons;
import common.Task;
import common.TaskFile;
import rpc.io.RpcClient;
import rpc.io.RpcServer;
import util.FileUtil;
import util.LogUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author razertory
 * @date 2021/1/1
 * dispatch task to worker
 */
public class Master extends RpcServer {
    private final Integer reduceNum;
    // 是否完成map
    private Boolean mapDone;
    // 任务是否完成
    private Boolean done;
    // 文件地址
    private String dir;
    private final RpcClient client;
    // 存储闲置注册的worker
    private final Map<Integer, WorkerDto> unUserWorkers;
    private final Map<Integer, WorkerDto> inProcessWorkers;
    // worker 与其List<Task>
    private final Map<WorkerDto, Future> workerWithTaskResult;
    // 存储已就绪的map task
    private final Map<Integer, Task> mapTasksReady;
    // 存储正在运行的map task
    private final Map<Integer, Task> mapTasksInProgress;
    // reduce task, similar as above
    private final Map<Integer, Task> reduceTasksReady;
    private final Map<Integer, Task> reduceTasksInProgress;

    public Master() {
        String dir = "mapreduce/src/main/resources/articles";
        List<String> files = FileUtil.readDirFile(dir);
        client = new RpcClient();
        // 初始化map和reduce task map
        mapTasksReady = new ConcurrentHashMap<>(16);
        mapTasksInProgress = new ConcurrentHashMap<>(16);
        reduceTasksReady = new ConcurrentHashMap<>(16);
        reduceTasksInProgress = new ConcurrentHashMap<>(16);
        // 初始化worker map
        unUserWorkers = new ConcurrentHashMap<>(16);
        inProcessWorkers = new ConcurrentHashMap<>(16);
        workerWithTaskResult = new ConcurrentHashMap<>(16);
        for (int i = 0; i < Objects.requireNonNull(files).size(); i++) {
            final Task task = new Task(i, Cons.TASK_TYPE_MAP,
                    new TaskFile(Cons.TASK_STATUS_TODO, dir + "/" + files.get(i),files.get(i)),
                    System.currentTimeMillis());
            mapTasksReady.put(i,task);
        }
        mapDone = false;
        done = false;
        reduceNum = 10;
    }

    /**
     * 开启server
     */
    public void start() {
        try {
            // start serve
            serve(Cons.MASTER_HOST);
            // 防止之前失败任务有遗留数据，先删除
            FileUtil.delete(Paths.get("map_tmp"));
            while (true) {
                // 如果还有资源且任务没完成的情况下
                int workerNum = unUserWorkers.size();
                if (workerNum > 0 && !done) {
                    // 将任务均匀分给对应worker
                    final Iterator<Map.Entry<Integer, WorkerDto>> iterator = unUserWorkers.entrySet().iterator();
                    while (iterator.hasNext()) {
                        final Map.Entry<Integer, WorkerDto> worker = iterator.next();
                        inProcessWorkers.put(worker.getKey(),worker.getValue());
                        iterator.remove();
                        final Future result = client
                                .call(worker.getValue().getPort(), "receiveTask", new Object[]{assignTask()});
                        // 存储当前worker执行的结果，后续异步调用
                        workerWithTaskResult.put(worker.getValue(), result);
                        System.out.println(result.get());
                    }
                } else if (done){
                    // 任务完成
                    LogUtil.log("Job Done!");
                    break;
                } else {
                    // 每两秒轮训，查看是否有可用资源进行任务分配
                    // todo 这里是否改为锁会更好一点
                    Thread.sleep(2000);
                }
            }
        } catch (Exception e) {
            LogUtil.log("fail to start master server on port: " + Cons.MASTER_HOST);
        }
    }


    /**
     * 指派任务
     * @return 要执行的task信息
     * todo 后续换为MRDto
     */
    public Task assignTask() {
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
//                return JSON.toJSONString(response);
                return response;
            }
        } else if (mapTasksInProgress.size() > 0) {
            response.setType(Cons.TASK_TYPE_WAIT);
//            return JSON.toJSONString(response);
            return response;
        }
        // reduce
        if (reduceTasksReady.size() > 0) {
            Set<Map.Entry<Integer, Task>> entrySet = reduceTasksReady.entrySet();
            for (Map.Entry<Integer, Task> entry : entrySet) {
                entry.getValue().setTimestamp(System.currentTimeMillis());
                response = entry.getValue();
                reduceTasksInProgress.put(entry.getKey(), entry.getValue());
                reduceTasksReady.remove(entry.getKey());
                LogUtil.log("Distribute reduce task: " + response.getId());
//                return JSON.toJSONString(response);
                return response;
            }
        } else if (reduceTasksInProgress.size() > 0) {
            response.setType(Cons.TASK_TYPE_WAIT);
        } else {
            response.setType(Cons.TASK_TYPE_ALL_DONE);
        }
        done = true;
//        return JSON.toJSONString(response);
        return response;
    }

    /**
     * 注册worker，由worker进行调用
     * @return 是否注册成功
     */
    public String registerWorker(WorkerDto workerDto) {
        unUserWorkers.put(workerDto.getWorkerId(),workerDto);
        LogUtil.log("Worker + " + workerDto.getWorkerId() + "register success");
        return "success";
    }

    /**
     * worker 执行完map task rpc调用该方法报告
     * @param task 执行完成的task
     * @return master是否相应成功
     */
    public String doneMapTask(Task task, WorkerDto currentWorker) {
        LogUtil.log("worker: " + currentWorker.getWorkerId() + " done map task");
        String result = "success";
        try {
            System.out.println(workerWithTaskResult.get(currentWorker).get());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        try {
            mapTasksInProgress.remove(task.getId());
            // 将当前worker添加至闲置
            inProcessWorkers.remove(currentWorker.getWorkerId());
            unUserWorkers.put(currentWorker.getWorkerId(),currentWorker);
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
        mapDone = true;
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

    public String doneReduceTask(Task task, WorkerDto currentWorker) {
        String result = "success";
        try {
            reduceTasksInProgress.remove(task.getId());
            // 将当前资源限制
            releaseWork(currentWorker);
            // 只有ready和in process的所有任务都处理完了，才能算reduce task finish
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
        // 清除临时文件
        try {
            FileUtil.delete(Paths.get("map_tmp"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将work状态置为限制
     * @param workerDto 要修改的work
     */
    private void releaseWork(WorkerDto workerDto) {
        inProcessWorkers.remove(workerDto.getWorkerId());
        unUserWorkers.put(workerDto.getWorkerId(),workerDto);
    }

    public static void main(String[] args) {
        final Master master = new Master();
        master.start();
    }

}
