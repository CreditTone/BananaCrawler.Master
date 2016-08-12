package com.banana.master.task;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.banana.master.RemoteDownload;
import com.banana.master.impl.CrawlerMasterServer;

import banana.core.PropertiesNamespace;
import banana.core.protocol.Task;
import banana.core.queue.BlockingRequestQueue;
import banana.core.queue.SimpleBlockingQueue;
import banana.core.request.BasicRequest;
import banana.core.request.StartContext;


public class TaskTracker {
	
	private static Logger logger = Logger.getLogger(TaskTracker.class);
	
	private String taskName;
	
	private String taskId;
	
	private Task config;
	
	private List<TaskDownloader> downloads = new ArrayList<TaskDownloader>();
	
	private BlockingRequestQueue requestQueue = new SimpleBlockingQueue();
	
	private StartContext context ;
	
	private Map<String,Object> properties = new HashMap<String,Object>();
	
	public TaskTracker(String name){
		taskName = name;
		taskId = taskName + "_" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		properties.put(PropertiesNamespace.Task.MAX_PAGE_RETRY_COUNT, 1);
	}

	public String getTaskName() {
		return taskName;
	}
	
	public String getId(){
		return taskId;
	}
	
	public void setConfig(Task task){
		config = task;
	}
	
	public StartContext getContext() {
		return context;
	}

	public void setContext(StartContext context) {
		this.context = context;
	}

	public Object getProperties(String propertie) {
		if (propertie == null){
			return config;
		}
		return properties.get(propertie);
	}
	
	public void start(int thread) throws Exception{
		downloads = CrawlerMasterServer.getInstance().elect(taskId, thread);
		if (downloads.isEmpty()){
			throw new Exception("Not set any downloader");
		}
		List<BasicRequest> seeds = context.getSeedRequests();
		pushRequests(seeds);
		for (TaskDownloader taskDownload : downloads) {
			
		}
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
