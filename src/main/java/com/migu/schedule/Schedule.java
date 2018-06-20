package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

    //创建节点集合
    private List<Integer> nodes = new ArrayList<Integer>();

    //创建任务集合
    private List<Integer> tasks = new ArrayList<Integer>();
   //
    private Map<Integer,Integer> taskMap = new HashMap<Integer,Integer>();

    Comparator<Integer> comparatorByTime = new Comparator<Integer>(){
        public int compare(Integer o1, Integer o2) {
            return (taskMap.get(o2)-taskMap.get(o1));
        }
    };

    private int threshold = 0;

    private Map<Integer,List<TaskInfo>> taskStatusMap = new HashMap<Integer, List<TaskInfo>>();

    private Map<Integer,List<Integer>> sameTasksMap = new HashMap<Integer, List<Integer>>();

    Comparator<TaskInfo> comparator = new Comparator<TaskInfo>(){
        public int compare(TaskInfo o1, TaskInfo o2) {
            return (o1.getTaskId()-o2.getTaskId());
        }
    };

    /**
     * 初始化成功，返回E001初始化成功
     * @return
     */
    public int init() {
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {

    //如果服务节点编号小于等于0, 返回E004:服务节点编号非法。
        if(nodeId<=0) {
            return ReturnCodeKeys.E004;
        }
        //如果服务节点编号已注册, 返回E005:服务节点已注册。
        if(nodes.contains(nodeId)){
            return ReturnCodeKeys.E005;
        }
        //注册成功，返回E003:服务节点注册成功。
        nodes.add(nodeId);
        Collections.sort(nodes);
        return ReturnCodeKeys.E003;

    }

    public int unregisterNode(int nodeId) {

        //如果服务节点编号未被注册, 返回E007:服务节点不存在。
        if(!nodes.contains(nodeId)) {
            return ReturnCodeKeys.E007;
        }
        //如果服务节点编号小于等于0, 返回E004:服务节点编号非法。
        if(nodeId<=0) {
            return ReturnCodeKeys.E004;
        }
        //注销成功，返回E006:服务节点注销成功。
        nodes.remove(new Integer(nodeId));
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {

        //如果任务编号小于等于0, 返回E009:任务编号非法。
        if(taskId<=0){
            return ReturnCodeKeys.E009;
        }
      //如果相同任务编号任务已经被添加, 返回E010:任务已添加。
        if(tasks.contains(taskId)){
            return ReturnCodeKeys.E010;
        }
      //   添加成功，返回E008任务添加成功。
        tasks.add(taskId);
        taskMap.put(taskId,consumption);
        Collections.sort(tasks,comparatorByTime);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        //如果任务编号小于等于0, 返回E009:任务编号非法。
        if(taskId<=0){
            return ReturnCodeKeys.E009;
        }
        //如果指定编号的任务未被添加, 返回E012:任务不存在。
        if(!tasks.contains(taskId)) {
            return ReturnCodeKeys.E012;
        }
        taskMap.remove(new Integer(taskId));
        tasks.remove(new Integer(taskId));
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {

        if(tasks.isEmpty()) {
            return ReturnCodeKeys.E014;
        }
        this.threshold = threshold;
        boolean balanced = false;

        return ReturnCodeKeys.E013;
    }

    private int countTasks(List<TaskInfo> taskInfos){
        int result = 0;
        for(TaskInfo taskInfo:taskInfos){
            result+=taskMap.get(taskInfo.getTaskId());
        }
        return result;
    }

    public int queryTaskStatus(List<TaskInfo> tasks) {

        for(Integer nodeId:taskStatusMap.keySet()){
            tasks.addAll(taskStatusMap.get(nodeId));
        }
        Collections.sort(tasks,comparator);
        System.out.println(tasks);
        // 如果查询结果参数tasks为null，返回E016:参数列表非法
        if(tasks==null){
            return ReturnCodeKeys.E016;
        }
        //未做此题返回 E000方法未实现。
        if(tasks.contains(tasks)){
            return ReturnCodeKeys.E000;
        }
        //如果查询成功, 返回E015: 查询任务状态成功;查询结果从参数Tasks返回。
        return ReturnCodeKeys.E015;

    }
}
