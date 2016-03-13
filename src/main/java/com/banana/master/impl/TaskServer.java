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

public class TaskServer {
	
	private static Logger logger = Logger.getLogger(TaskServer.class);
	
	private String taskName;
	
	private BlockingRequestQueue requestQueue = new SimpleBlockingQueue();
	
	private StartContext context ;
	
	private Collection<RemoteDownload> remoteDownload = CrawlerMasterServer.getInstance().getAllDownload();
	
	private Map<String,Object> properties = new HashMap<String,Object>();
	
	public TaskServer(String name){
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
	

	public void start(){
		for (RemoteDownload rd : remoteDownload) {
			
		}
	}
	
	public void setRequestQueue(BlockingRequestQueue queue){
		this.requestQueue = queue;
	}
	

	public void pushRequests(List<BasicRequest> requests) {
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
