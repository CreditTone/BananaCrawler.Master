package banana.master.task;

import banana.core.exception.DownloadException;
import banana.core.modle.CommandResponse;
import banana.core.modle.TaskStatus;
import banana.core.modle.TaskStatus.DownloaderTrackerStatus;
import banana.core.request.Cookies;
import banana.master.RemoteDownload;

public class RemoteDownloaderTracker {
	
	private int threadNum;
	
	private RemoteDownload owner;
	
	private TaskTracker taskTracker;
	
	private boolean isValid = false;
	
	public RemoteDownloaderTracker(int threadNum, RemoteDownload owner) {
		this.threadNum = threadNum;
		this.owner = owner;
	}

	public RemoteDownloaderTracker(int threadNum, RemoteDownload owner,TaskTracker taskTracker) {
		this.threadNum = threadNum;
		this.owner = owner;
		this.taskTracker = taskTracker;
	}
	
	public TaskTracker getTaskTracker() {
		return taskTracker;
	}

	public void setTaskTracker(TaskTracker taskTracker) {
		this.taskTracker = taskTracker;
	}
	
	public int getWorkThread(){
		return threadNum;
	}
	
	public String getIp(){
		return owner.getIp();
	}
	
	public int getPort(){
		return owner.getPort();
	}
	
	public boolean isStoped() {
		return isValid;
	}
	
	public void start()throws DownloadException{
		start(null);
	}

	public void start(Cookies initCookie) throws DownloadException{
		String taskId = taskTracker.getId();
		owner.startDownloadTracker(taskId, taskTracker.getConfig(), initCookie);
		isValid = true;
	}
	
	public void stop() throws DownloadException{
		String taskId = taskTracker.getId();
		owner.stopDownloadTracker(taskId);
		isValid = false;
	}
	
	public boolean isWaitRequest(){
		String taskId = taskTracker.getId();
		try {
			return owner.isWaitRequest(taskId);
		} catch (DownloadException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public void updateConfig(int thread){
		try {
			owner.resubmitTaskConfig(taskTracker.getId(), thread, taskTracker.getConfig());
		} catch (DownloadException e) {
			e.printStackTrace();
		}
	}
	
	public DownloaderTrackerStatus getStatus(){
		DownloaderTrackerStatus status = new DownloaderTrackerStatus();
		status.thread = threadNum;
		status.owner = owner.getIp() + ":" + owner.getPort();
		status.stat = TaskStatus.Stat.Runing;
		return status;
	}
	
	public void injectCookies(Cookies cookies) throws DownloadException{
		owner.injectCookies(taskTracker.getId(), cookies);
	}
}
