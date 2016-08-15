package com.banana.master.task;

import java.io.Closeable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.banana.master.RemoteDownload;
import com.banana.master.impl.CrawlerMasterServer;

import banana.core.PropertiesNamespace;
import banana.core.exception.DownloadException;
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
	
	private int allThread;
	
	private List<RemoteDownloaderTracker> downloads = new ArrayList<RemoteDownloaderTracker>();
	
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
	
	public Task getConfig(){
		return config;
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
		logger.info(String.format("%s 分配了%d个Downloader", taskId, downloads.size()));
		for (RemoteDownloaderTracker rdt : downloads) {
			logger.info(String.format("%s Downloader %s Thread %d", taskId, rdt.getIp(), rdt.getWorkThread()));
		}
		allThread = thread;
		if (downloads.isEmpty()){
			throw new Exception("Not set any downloader");
		}
		List<BasicRequest> seeds = context.getSeedRequests();
		for (BasicRequest req : seeds) {
			pushRequest(req);
		}
		for (RemoteDownloaderTracker taskDownload : downloads) {
			taskDownload.setTaskTracker(this);
			taskDownload.start();
		}
	}
	
	public void setRequestQueue(BlockingRequestQueue queue){
		this.requestQueue = queue;
	}
	

	public void pushRequest(BasicRequest request) {
		logger.info(taskName+" push requests " + request);
		request.recodeRequest();
		requestQueue.add(request);
	}
	

	public final BasicRequest pollRequest() throws InterruptedException{
		BasicRequest req = null;
		for (int i = 0; i < 3; i++) {
			req = requestQueue.poll();
			if (req != null)
				break;
			Thread.sleep(100);
		}
		return req;
	}
	
	public void removeRemoteDownload(String ip,int port){
		for (int x = 0 ;x < downloads.size() ; x++) {
			RemoteDownloaderTracker rdt = downloads.get(x);
			if (rdt.getIp().equals(ip) && rdt.getPort() == port){
				downloads.remove(x);
				requestNewDownload(rdt.getWorkThread());
				break;
			}
		}
	}
	
	private void requestNewDownload(int diffThread){
		List<RemoteDownload> allDownloads = CrawlerMasterServer.getInstance().getDownloads();
		if (downloads.size() < allDownloads.size()){
			for (int i = 0; i < allDownloads.size() * 3; i++) {
				int index = new Random().nextInt(allDownloads.size());
				RemoteDownload newDownloader = allDownloads.get(index);
				if (!containDownload(newDownloader.getIp(), newDownloader.getPort())){
					RemoteDownloaderTracker newRemoteDownloaderTracker = new RemoteDownloaderTracker(diffThread, newDownloader, this);
					try {
						newRemoteDownloaderTracker.start();
						downloads.add(newRemoteDownloaderTracker);
					} catch (DownloadException e) {
						e.printStackTrace();
					}
					break;
				}
			}
		}else{
			//平均向各个DownloadTracker追加线程
			System.out.println("平均向各个DownloadTracker追加线程");
		}
	}
	
	
	private boolean containDownload(String ip,int port){
		for (RemoteDownloaderTracker rdt : downloads) {
			if (rdt.getIp().equals(ip) && rdt.getPort() == port){
				return true;
			}
		}
		return false;
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
