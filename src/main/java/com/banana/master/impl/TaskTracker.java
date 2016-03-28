package com.banana.master.impl;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.banana.common.PropertiesNamespace;
import com.banana.component.DynamicEntrance;
import com.banana.component.listener.TaskLifeListener;
import com.banana.exception.EntranceException;
import com.banana.queue.BlockingRequestQueue;
import com.banana.queue.RequestPriorityBlockingQueue;
import com.banana.queue.SimpleBlockingQueue;
import com.banana.request.BasicRequest;
import com.banana.request.StartContext;

public class TaskTracker {
	
	private static Logger logger = Logger.getLogger(TaskTracker.class);
	
	private String taskName;
	
	private BlockingRequestQueue requestQueue = new SimpleBlockingQueue();
	
	private StartContext context ;
	
	private RemoteDownload remoteDownload = new RemoteDownload();
	
	private List<String> downloadHosts = new ArrayList<String>();
	
	private Map<String,Object> properties = new HashMap<String,Object>();
	
	public TaskTracker(String name){
		taskName = name;
		properties.put(PropertiesNamespace.Task.MAX_PAGE_RETRY_COUNT, 1);
	}

	public String getTaskName() {
		return taskName;
	}
	
	public StartContext getContext() {
		return context;
	}

	public void setContext(StartContext context) {
		this.context = context;
	}

	public Map<String,Object> getProperties() throws RemoteException {
		return properties;
	}
	
	public void addDownloadHost(String host){
		downloadHosts.add(host);
	}
	
	public int getDownloadCount(){
		return downloadHosts.size();
	}
	

	public void start(int thread) throws Exception{
		if (downloadHosts.isEmpty()){
			throw new Exception("Not set any downloader");
		}
		remoteDownload.setDownloads(downloadHosts);
		int index = 0;
		while(thread > 0){
			remoteDownload.addDownloadThread(downloadHosts.get(index), 1);
			thread	--;
			index 	++;
			if (index == downloadHosts.size()){
				index = 0;
			}
		}
		List<BasicRequest> seeds = context.getSeedRequests();
		pushRequests(seeds);
		remoteDownload.startCrawl(taskName);
	}
	
	public void setRequestQueue(BlockingRequestQueue queue){
		this.requestQueue = queue;
	}
	

	public void pushRequests(List<BasicRequest> requests) {
		logger.info("task "+taskName+"push requests "+requests);
		for (BasicRequest req : requests) {
			req.recodeRequest();
			requestQueue.add(req);
		}
	}
	

	public final List<BasicRequest> pollRequest(int fetchsize) throws InterruptedException{
		List<BasicRequest> reqs = new ArrayList<BasicRequest>();
		while (true) {
			BasicRequest req = requestQueue.poll();
			if (req == null)
				break;
			reqs.add(req);
			if (reqs.size() >= fetchsize){
				break;
			}
		}
		return reqs;
	}
	
	

	/**
	 * 任务完成销毁任务
	 */
	private final void destoryCrawlTask(){
		//释放队列
		if(requestQueue instanceof Closeable){
			Closeable closeable = (Closeable) requestQueue;
			try {
				closeable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		logger.info(taskName+"完成销毁");
	}
}
