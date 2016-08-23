package com.banana.master.task;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.banana.master.impl.CrawlerMasterServer;

import banana.core.PropertiesNamespace;
import banana.core.exception.DownloadException;
import banana.core.filter.Filter;
import banana.core.filter.SimpleBloomFilter;
import banana.core.protocol.Task;
import banana.core.queue.BlockingRequestQueue;
import banana.core.queue.SimpleBlockingQueue;
import banana.core.request.HttpRequest;
import banana.core.request.StartContext;

public class TaskTracker {

	private static Logger logger = Logger.getLogger(TaskTracker.class);

	private String taskId;

	private Task config;

	private List<RemoteDownloaderTracker> downloads = new ArrayList<RemoteDownloaderTracker>();

	private BlockingRequestQueue requestQueue = new SimpleBlockingQueue();

	private StartContext context;

	private Map<String, Object> properties = new HashMap<String, Object>();

	private Filter filter = null;

	public TaskTracker(Task taskConfig) {
		config = taskConfig;
		initFilter();
		taskId = taskConfig.name + "_" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		properties.put(PropertiesNamespace.Task.MAX_PAGE_RETRY_COUNT, 1);
	}

	private void initFilter() {
		if (config.filter != null && config.filter.length() > 0) {
			switch (config.filter) {
			case "simple":
				filter = new SimpleBloomFilter();
				break;
			default:
				filter = null;
			}
		} else {
			filter = null;
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

	public void setContext(StartContext context) {
		this.context = context;
	}

	public Object getProperties(String propertie) {
		if (propertie == null) {
			return config;
		}
		return properties.get(propertie);
	}

	public void start(int thread) throws Exception {
		downloads = CrawlerMasterServer.getInstance().elect(taskId, thread);
		logger.info(String.format("%s 分配了%d个Downloader", taskId, downloads.size()));
		for (RemoteDownloaderTracker rdt : downloads) {
			logger.info(String.format("%s Downloader %s Thread %d", taskId, rdt.getIp(), rdt.getWorkThread()));
		}
		if (downloads.isEmpty()) {
			throw new Exception("Not set any downloader");
		}
		List<HttpRequest> seeds = context.getSeedRequests();
		for (HttpRequest req : seeds) {
			pushRequest(req);
		}
		for (RemoteDownloaderTracker taskDownload : downloads) {
			taskDownload.setTaskTracker(this);
			taskDownload.start();
		}
	}

	public void updateConfig(Task taskConfig) throws Exception {
		int diffNum = taskConfig.thread - config.thread;
		config = taskConfig;
		initFilter();
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
		if (filter != null) {
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
			destoryTask();
		}
		return req;
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
	private final void destoryTask() {
		for (RemoteDownloaderTracker taskDownload : downloads) {
			try {
				taskDownload.stop();
			} catch (DownloadException e) {
				e.printStackTrace();
			}
		}
		// 释放队列
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
	}
}
