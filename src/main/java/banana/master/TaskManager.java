package banana.master;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import banana.core.protocol.Task;
import banana.master.task.TaskTimer;
import banana.master.task.TaskTracker;

public class TaskManager {

	private Map<String, TaskTracker> id2Task = new HashMap<String, TaskTracker>();

	private Map<String, TaskTracker> name2Task = new HashMap<String, TaskTracker>();

	private Map<String, TaskTimer> name2Tasktimer = new HashMap<String, TaskTimer>();

	private Map<String, Task> name2Prepared = new HashMap<String, Task>();

	public TaskTracker getTaskTrackerById(String taskid) {
		return id2Task.get(taskid);
	}

	public TaskTracker getTaskTrackerByName(String taskname) {
		return name2Task.get(taskname);
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
		id2Task.put(taskTracker.getId(), taskTracker);
		name2Task.put(taskTracker.getTaskName(), taskTracker);
	}
	
	public void addPreparedTask(Task task) {
		name2Prepared.put(task.name, task);
	}

	public void removeTaskTracker(String name) {
		id2Task.remove(name2Task.remove(name).getId());
	}

	public Collection<TaskTracker> allTaskTracker() {
		return id2Task.values();
	}

	public Collection<TaskTimer> allTaskTimer() {
		return name2Tasktimer.values();
	}
}
