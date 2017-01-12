package banana.master;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import banana.core.protocol.Task;
import banana.master.task.TaskTimer;
import banana.master.task.TaskTracker;

public class TaskManager {

	private Map<String, TaskTracker> id2TaskTracker = new HashMap<String, TaskTracker>();

	private Map<String, TaskTimer> name2Tasktimer = new HashMap<String, TaskTimer>();

	private Map<String, Task> name2Prepared = new HashMap<String, Task>();

	public TaskTracker getTaskTrackerById(String taskid) {
		return id2TaskTracker.get(taskid);
	}

	public TaskTimer getTaskTimerByName(String taskname) {
		return name2Tasktimer.get(taskname);
	}
	
	public Task getPreparedTaskByName(String taskname) {
		return name2Prepared.get(taskname);
	}

	public boolean existTimer(String taskname) {
		return name2Tasktimer.get(taskname) != null;
	}
	
	public boolean existPrepared(String taskname) {
		return name2Prepared.get(taskname) != null;
	}

	public void addTaskTimer(TaskTimer tasktimer) {
		name2Tasktimer.put(tasktimer.cfg.name, tasktimer);
	}

	public void addTaskTracker(TaskTracker taskTracker) {
		id2TaskTracker.put(taskTracker.getId(), taskTracker);
	}
	
	public void addPreparedTask(Task task) {
		name2Prepared.put(task.name, task);
	}

	public void removeTaskTrackerById(String id) {
		id2TaskTracker.remove(id);
	}

	public Collection<TaskTracker> allTaskTracker() {
		return id2TaskTracker.values();
	}

	public Collection<TaskTimer> allTaskTimer() {
		return name2Tasktimer.values();
	}
	
	public Collection<TaskTracker> getTaskTrackerByName(String taskname){
		List<TaskTracker> trackers = new ArrayList<>();
		for (TaskTracker tracker : id2TaskTracker.values()) {
			if (tracker.getConfig().name.equals(taskname)){
				trackers.add(tracker);
			}
		}
		return trackers;
	}
	
	public void verify(Task config) throws Exception {
		if (name2Tasktimer.containsKey(config.name)){
			throw new Exception("存在名字相同的任务定时器 "+  config.name);
		}
		if (name2Prepared.containsKey(config.name)){
			throw new Exception("存在名字相同的与准备任务 "+  config.name);
		}
		for (TaskTracker tracker : getTaskTrackerByName(config.name)) {
			if (tracker.getConfig().allow_multi_task != config.allow_multi_task){
				throw new Exception("存在名字相同的任务，但配置allow_multi_task不同");
			}else if(config.allow_multi_task == false){
				throw new Exception("名字为" + config.name +" 已经存在。如果想多任务运行，请设置allow_multi_task为true");
			}
		}
	}
}
