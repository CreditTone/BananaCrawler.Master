package com.banana.master.task;

import com.banana.master.RemoteDownload;

public class TaskDownloader {
	
	private int threadNum;
	
	private RemoteDownload owner;

	public TaskDownloader(int threadNum, RemoteDownload owner) {
		this.threadNum = threadNum;
		this.owner = owner;
	}
	
	
}
