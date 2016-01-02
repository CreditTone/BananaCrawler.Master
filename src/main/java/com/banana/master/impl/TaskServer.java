package com.banana.master.impl;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
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
import com.banana.request.BasicRequest;
import com.banana.request.StartContext;
import com.banana.request.PageRequest.PageEncoding;

public class TaskServer implements Runnable {
	
	private static Logger logger = Logger.getLogger(TaskServer.class);
	
	private String taskName;
	
	private BlockingQueue<StartContext> startRequests = new LinkedBlockingQueue<StartContext>();
	
	/**
	 * 默认用无延迟、优先级队列
	 */
	private BlockingRequestQueue requestQueue = new RequestPriorityBlockingQueue();
	
	/**
	 * 备份初始URL
	 */
	private List<StartContext> allStartBackups = new ArrayList<StartContext>();
	
	/**
	 * 动态入口URL
	 */
	private DynamicEntrance dynamicEntrance;
	
	/**
	 * 生命周期监听类
	 */
	private TaskLifeListener taskLifeListener;
	
	
	/**
	 * 临时变量，记录任务开始注入口URL个数
	 */
	private int seedCount;
	
	private StartContext context;
	
	private RemoteDownload remoteDownload = new RemoteDownload();
	
	private Map<String,Object> properties = new HashMap<String,Object>();
	
	public TaskServer(String name){
		taskName = name;
		properties.put(PropertiesNamespace.Task.MAX_PAGE_RETRY_COUNT, 1);
		new Thread(this).start();
	}

	public Map<String,Object> getProperties() throws RemoteException {
		return properties;
	}
	
	/**
	 * 清除所有初始URL  
	 * 在监听任务里初始化入口URL前调用该方法释放之前的入口url
	 */
	private void clearStartRequest(){
		startRequests.clear();
		allStartBackups.clear();
	}

	public void pushRequests(List<BasicRequest> requests) throws RemoteException {
		for (BasicRequest req : requests) {
			req.recodeRequest();
			requestQueue.add(req);
		}
	}
	
	public void addStartContxt(StartContext context)
	{
		if(context.isEmpty()){
			throw new EntranceException("StartContext必须至少有1个seed");
		}
		allStartBackups.add(context);
		seedCount+=context.getSeedSize();
	}
	
	/**
	 * 每个入口URL及子队列全部抓取完成则返回true
	 * @return
	 */
	public boolean isSingleStartFinished(){
		int alive = 0;
		int offline = 0;
//		int alive = downloadThreadPool.getThreadAlive();这个task所有download link的活跃线程数
//		int offline = offlineHandleThreadPool.getThreadAlive();这个task所有offlineHandle的活跃线程数
		if(alive == 0 && requestQueue.isEmpty() && offline == 0){
			return true;
		}
		return false;
	}
	
	public final BasicRequest poolRequest() throws InterruptedException{
		BasicRequest req = null;
		while (true) {
			
//			if(看看download积压link的数量有没有达到阈值（每个download设置阈值）){
//				Thread.sleep(100);
//				continue;//等待有线程可以工作
//			}
			if ((!requestQueue.isEmpty() || !isSingleStartFinished())) {
				req = requestQueue.poll();
				if (req != null)
					break;
				else 
					Thread.sleep(100);
			} else {
				break;
			}
		}
		return req;
	}

	/**
	 * 从备份开始库拷贝到开始队列
	 */
	private void copyStartBackupToStartQueue() {
		if(startRequests.isEmpty()){
			for (StartContext context: allStartBackups) {//开始队列加载种子URL
				startRequests.add(context);
			}
		}
	}
	
	/**
	 * 从入口URL队列取得一个URL  如果为Null则说明这批入口已经抓完。去询问动态入口实例是否需要加载新的入口URL
	 * @return
	 */
	private boolean nextStartUrlQueue() {
		if(startRequests.isEmpty() && dynamicEntrance != null){
			if(dynamicEntrance.continueLoad()){
				if(dynamicEntrance.isClearLast()){
					clearStartRequest();//清除之前的入口URL
				}
				initDynamicEntrance();
				copyStartBackupToStartQueue();
			}
		}
		context = startRequests.poll();
		if(context != null && !context.isEmpty()){
			List<BasicRequest> subRequest = context.getSeedRequests();
			try {
				pushRequests(subRequest);
			} catch (RemoteException e) {
			}
		}
		return context!=null;
	}
	
	/**
	 * 初始化任务
	 */
	private void initTask() throws EntranceException{
		logger.info("正在注入口信息....");
		initDynamicEntrance();
		if(allStartBackups.isEmpty()){
			throw new EntranceException("种子URL数至少有1个");
		}
		copyStartBackupToStartQueue();
		logger.info("注入StartContext"+startRequests.size()+"个"); 
		logger.info("注入种子URL"+seedCount+"个"); 
		if(taskLifeListener != null){
			taskLifeListener.onStart(taskName);
		}
		nextStartUrlQueue();
	}
	
	/**
	 * 动态加载入口URL
	 */
	private final void initDynamicEntrance() {
		if(dynamicEntrance != null){
			List<StartContext> startContexts = dynamicEntrance.loadStartContext();
			if(startContexts != null){
				for (StartContext sc : startContexts) {
					addStartContxt(sc);
				}
			}
		}
	}
	
	public void run() {
		logger.info("开始抓取");
		try{
			initTask();
		}catch(EntranceException e){
			destoryCrawlTask();
			//任务生命周期回调
			if(taskLifeListener != null){
				taskLifeListener.onFinished(taskName);
			}
			throw e;
		}
		
		while(!Thread.currentThread().isInterrupted()){
			BasicRequest request ;
			try{
				request = poolRequest();
				if(request == null){
					if(isSingleStartFinished() && !nextStartUrlQueue()){//如果当前入口URL抓完并且没有了下一个入口URL则完成任务
						destoryCrawlTask();
						break;
					}else{
						continue;
					}
				}
			}catch(Exception e){
				e.printStackTrace();
				logger.error("轮询队列出错",e);
				break;
			}
			final BasicRequest finalRequest = request;
			final StartContext finalContext  = context;
			remoteDownload.download(taskName, finalRequest, finalContext);
		}
	}
	
	public RemoteDownload getRemoteDownload(){
		return remoteDownload;
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
		
		//spider.destoryCrawTask(taskName);//销毁任务
		
		//任务生命周期回调
		if(taskLifeListener != null){
			taskLifeListener.onFinished(this.taskName);
		}
		logger.info(taskName+"完成销毁");
	}
}
