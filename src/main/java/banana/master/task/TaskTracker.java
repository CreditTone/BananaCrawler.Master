package banana.master.task;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import banana.core.exception.DownloadException;
import banana.core.filter.Filter;
import banana.core.filter.NotFilter;
import banana.core.filter.SimpleBloomFilter;
import banana.core.protocol.Task;
import banana.core.queue.BlockingRequestQueue;
import banana.core.queue.RequestQueueBuilder;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.StartContext;
import banana.core.util.SystemUtil;
import banana.master.impl.CrawlerMasterServer;

public class TaskTracker {
	
	public static final int RUN_MODE = 0;
	
	public static final int TEST_MODE = 1;
	
	public static int MODE = RUN_MODE;

	private static Logger logger = Logger.getLogger(TaskTracker.class);

	private String taskId;

	private Task config;

	private List<RemoteDownloaderTracker> downloads = new ArrayList<RemoteDownloaderTracker>();

	private BlockingRequestQueue requestQueue;

	private StartContext context;

	private Filter filter = null;
	
	private int loopCount = 0;
	
	private BackupRunnable backupRunnable;
	
	public TaskTracker(Task taskConfig) {
		config = taskConfig;
		taskId = taskConfig.name + "_" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		context = new StartContext();
		initSeed(config.seeds);
		initFilter(config.filter);
		initQueue(config.queue);
		setBackup();
		initSeedRequest();
		initPreviousState(config.synchronizeStat, config.name, config.collection);
		logger.info(String.format("TaskTracker %s use filter %s queue %s", taskId, filter.getClass().getName(), requestQueue.getClass().getName()));
		Iterator<HttpRequest> iter = requestQueue.iterator();
		while(iter.hasNext()) {
			logger.info(String.format("seed %s", iter.next().getUrl()));
		}
	}
	
	private void setBackup(){
		if (MODE == TEST_MODE){
			return;
		}
		if (backupRunnable == null){
			backupRunnable = new BackupRunnable();
		}
		backupRunnable.setConfig(config);
		backupRunnable.setContext(context);
		backupRunnable.setFilter(filter);
		backupRunnable.setRequestQueue(requestQueue);
	}
	
	private void initSeed(List<Task.Seed> seeds){
		for (Task.Seed seed : seeds) {
			String[] urls = null;
			if (seed.getUrl() != null){
				urls = new String[]{seed.getUrl()};
			}else{
				urls = seed.getUrls();
			}
			for (int i = 0; i < urls.length; i++) {
				PageRequest req = context.createPageRequest(urls[i], seed.getProcessor());
				if (seed.getMethod() == null || "GET".equalsIgnoreCase(seed.getMethod())){
					req.setMethod(HttpRequest.Method.GET);
				}else{
					req.setMethod(HttpRequest.Method.POST);
					Map<String,String> params = seed.getParams();
					for (Map.Entry<String, String> valuePair : params.entrySet()){
						req.putParams(valuePair.getKey(), valuePair.getValue());
					}
				}
				if (seed.getHeaders() != null){
					for (Map.Entry<String, String> valuePair : seed.getHeaders().entrySet()) {
						req.putHeader(valuePair.getKey(), valuePair.getValue());
					}
				}
				context.injectSeed(req);
			}
		}
	}
	
	private void initFilter(Task.Filter filtercfg){
		filter = new NotFilter();
		if (filtercfg.type != null && filtercfg.type.length() > 0) {
			switch (filtercfg.type) {
			case "simple":
				filter = new SimpleBloomFilter();
				break;
			}
		}
	}
	
	private void initQueue(Map<String,Object> queuecfg){
		int delay = 0;
		boolean suportPriority = false;
		if (queuecfg.containsKey("delay")){
			delay = (int) queuecfg.get("delay");
		}
		if (queuecfg.containsKey("suport_priority")){
			suportPriority = (boolean) queuecfg.get("suport_priority");
		}
		RequestQueueBuilder builder = new RequestQueueBuilder()
				.setDelayPeriod(delay)
				.setSuportPriority(suportPriority);
		requestQueue = builder.build();
	}
	

	private void initPreviousState(boolean synchronizeStat,String name,String collection) {
		if (synchronizeStat){
			GridFS tracker_status = new GridFS(CrawlerMasterServer.getInstance().db,"tracker_stat");
			GridFSDBFile file = tracker_status.findOne(name + "_" + collection + "_filter");
			if (file != null){
				byte[] filterData = SystemUtil.inputStreamToBytes(file.getInputStream());
				System.out.println("filterData len = " + filterData.length);
				filter.load(filterData);
			}
			file = tracker_status.findOne(name + "_" + collection + "_context");
			if (file != null){
				byte[] contextData = SystemUtil.inputStreamToBytes(file.getInputStream());
				System.out.println("contextData len = " + contextData.length);
				context.load(contextData);
			}
			file = tracker_status.findOne(name + "_" + collection + "_links");
			if (file != null){
				byte[] data = SystemUtil.inputStreamToBytes(file.getInputStream());
				System.out.println("linksData len = " + data.length);
				requestQueue.load(new ByteArrayInputStream(data));
			}
		}
	}
	
	public String getTaskName() {
		return config.name;
	}

	public String getId() {
		return taskId;
	}

	public Task getConfig() {
		return config;
	}

	public StartContext getContext() {
		return context;
	}
	
	public Object getProperties(String propertie) {
		if (propertie == null) {
			return config;
		}
		Object result = context.getContextAttribute(propertie);
		if (result instanceof String){
			return new Text((String) result);
		}
		return result;
	}
	
	private void initSeedRequest() {
		List<HttpRequest> seeds = context.getSeedRequests();
		for (HttpRequest req : seeds) {
			requestQueue.add(req);
		}
	}

	public void start() throws Exception {
		downloads = CrawlerMasterServer.getInstance().elect(taskId, config.thread);
		logger.info(String.format("%s 分配了%d个Downloader", taskId, downloads.size()));
		for (RemoteDownloaderTracker rdt : downloads) {
			logger.info(String.format("%s Downloader %s Thread %d", taskId, rdt.getIp(), rdt.getWorkThread()));
		}
		if (downloads.isEmpty()) {
			throw new Exception("Not set any downloader");
		}
		for (RemoteDownloaderTracker taskDownload : downloads) {
			taskDownload.setTaskTracker(this);
			taskDownload.start();
		}
	}

	public void updateConfig(Task taskConfig) throws Exception {
		int diffNum = taskConfig.thread - config.thread;
		if (!config.filter.equals(taskConfig.filter)){
			initFilter(taskConfig.filter);
			backupRunnable.setFilter(filter);
		}
		if (!config.queue.equals(taskConfig.queue)){
			initQueue(taskConfig.queue);
		}
		config = taskConfig;
		if (diffNum == 0) {
			for (RemoteDownloaderTracker rdt : downloads) {
				rdt.updateConfig(taskConfig.thread);
			}
			return;
		}
		int absNum = Math.abs(diffNum);
		int[] threads = new int[downloads.size()];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = downloads.get(i).getWorkThread();
		}
		if (diffNum < 0) {
			for (int j = 0; (j < threads.length && absNum > 0); j++) {
				if (threads[j] == 0) {
					continue;
				}
				if (absNum >= threads[j]) {
					threads[j] = 0;
					absNum -= threads[j];
				} else {
					threads[j] -= absNum;
					absNum = 0;
				}
			}
			for (int i = 0; i < threads.length; i++) {
				if (threads[i] == 0) {
					try {
						downloads.get(i).stop();
					} catch (DownloadException e) {
						logger.info(String.format("%s stop RemotedownloadTracker %s failure", taskId,
								downloads.get(i).getIp()), e);
					}
				} else {
					downloads.get(i).updateConfig(threads[i]);
				}
			}
			Iterator<RemoteDownloaderTracker> iter = downloads.iterator();
			while (iter.hasNext()) {
				RemoteDownloaderTracker rdt = iter.next();
				if (!rdt.isStoped()) {
					downloads.remove(rdt);
				}
			}
		} else {
			List<RemoteDownloaderTracker> newTrackers = CrawlerMasterServer.getInstance().electAgain(downloads,
					diffNum);
			if (newTrackers != null) {
				downloads.addAll(newTrackers);
				for (RemoteDownloaderTracker remoteDownloader : newTrackers) {
					try {
						remoteDownloader.setTaskTracker(this);
						remoteDownloader.start();
					} catch (DownloadException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	public void setRequestQueue(BlockingRequestQueue queue) {
		this.requestQueue = queue;
	}

	public void pushRequest(HttpRequest request) {
		if (config.filter.target.contains(request.getProcessor())){
			if (filter.contains(request.getUrl())){
				logger.info(String.format("%s filter request %s", taskId, request.getUrl()));
				return;
			}else{
				filter.add(request.getUrl());
			}
		}
		logger.info(String.format("%s push request %s", taskId, request.getUrl()));
		request.recodeRequest();
		requestQueue.add(request);
	}

	public final HttpRequest pollRequest() throws InterruptedException {
		HttpRequest req = null;
		for (int i = 0; i < 3; i++) {
			req = requestQueue.poll();
			if (req != null)
				return req;
			Thread.sleep(100);
		}
		if (isAllWaiting() && requestQueue.isEmpty()) {
			synchronized (this) {
				System.out.println("synchronized tasktracker " + Thread.currentThread().getName());
				if (requestQueue.isEmpty()){
					loopCount ++;
					if (loopCount >= config.loops){
						destoryTask();
					}else{
						logger.info(String.format("finish loop The %d times", loopCount));
						initSeedRequest();
					}
				}
			}
		}
		return requestQueue.poll();
	}
	
	public synchronized boolean filterQuery(String ... fields){
		for (int i = 0; i < fields.length; i++) {
			if (!filter.contains(fields[i])){
				return false;
			}
		}
		return true;
	}
	
	public void addFilter(String ... fields){
		for (int i = 0; i < fields.length; i++) {
			filter.add(fields[i]);
		}
	}

	private boolean isAllWaiting() {
		for (RemoteDownloaderTracker rdt : downloads) {
			if (!rdt.isWaitRequest()) {
				return false;
			}
		}
		return true;
	}

	public boolean containDownload(String ip, int port) {
		for (RemoteDownloaderTracker rdt : downloads) {
			if (rdt.getIp().equals(ip) && rdt.getPort() == port) {
				return true;
			}
		}
		return false;
	}
	/**
	 * 任务完成销毁任务
	 */
	public final void destoryTask() {
		new Thread(){
			public void run() {
				if (downloads == null){
					return;
				}
				for (RemoteDownloaderTracker taskDownload : downloads) {
					try {
						taskDownload.stop();
					} catch (DownloadException e) {
						e.printStackTrace();
					}
				}
				logger.info("downloaderTracker closed");
				if (backupRunnable != null){
					logger.info("begin backup stats");
					backupRunnable.close();
					logger.info("backup stats finished");
				}
				if (requestQueue instanceof Closeable) {
					Closeable closeable = (Closeable) requestQueue;
					try {
						closeable.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				CrawlerMasterServer.getInstance().removeTask(taskId);
				logger.info(config.name + " 完成销毁");
				downloads = null;
			};
		}.start();
	}
}
