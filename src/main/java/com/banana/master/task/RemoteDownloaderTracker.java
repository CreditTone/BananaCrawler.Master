package com.banana.master.task;

import com.banana.master.RemoteDownload;

import banana.core.exception.DownloadException;

public class RemoteDownloaderTracker {
	
	private int threadNum;
	
	private RemoteDownload owner;
	
	private TaskTracker taskTracker;
	
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

	public void start() throws DownloadException{
		String taskId = taskTracker.getId();
		owner.getDownloadProtocol().startDownloadTracker(taskId, threadNum);
	}
	
}
