package com.banana.master.impl;

import java.io.StringReader;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

import com.banana.common.JOperator;
import com.banana.common.JOperator.OperationJedis;
import com.banana.common.NodeStatus;
import com.banana.common.PrefixInfo;
import com.banana.common.PropertiesNamespace;
import com.banana.common.download.IDownload;
import com.banana.common.master.ICrawlerMasterServer;
import com.banana.component.config.XmlConfigPageProcessor;
import com.banana.queue.DelayedBlockingQueue;
import com.banana.queue.DelayedPriorityBlockingQueue;
import com.banana.queue.RequestPriorityBlockingQueue;
import com.banana.queue.SimpleBlockingQueue;
import com.banana.request.BasicRequest;
import com.banana.request.PageRequest;
import com.banana.request.StartContext;
import com.banana.util.PatternUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public final class CrawlerMasterServer extends UnicastRemoteObject implements ICrawlerMasterServer,Runnable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(CrawlerMasterServer.class);
	
	private static CrawlerMasterServer master = null;
	
	private long heartCheckInterval = 1000 * 10;
	
	private Map<String,Object> masterProperties = new HashMap<String,Object>();
	
	private Map<String,TaskServer> tasks = new HashMap<String, TaskServer>();
	
	private Map<String,IDownload> downloads = new HashMap<String,IDownload>();
	
	private Map<IDownload,NodeStatus> lastNodeStatus = new HashMap<IDownload,NodeStatus>();
	
	private Map<IDownload,Integer> weights = new HashMap<IDownload,Integer>();
	
	private JOperator redis;
	
	protected CrawlerMasterServer() throws RemoteException {
		super();
	}
	
	public static void init(final String redisHost,final int redisPort){
			try {
				master = new CrawlerMasterServer();
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			master.redis = JOperator.newInstance(redisHost, redisPort);
			master.masterProperties.put(PropertiesNamespace.Master.REDIS_HOST, redisHost);
			master.masterProperties.put(PropertiesNamespace.Master.REDIS_PORT, redisPort);
	}
	
	public static CrawlerMasterServer getInstance(){
		return master;
	}

	public long getHeartCheckInterval() {
		return heartCheckInterval;
	}

	public void registerDownloadNode() throws java.rmi.RemoteException {
		try {
			String rmiAddress = "rmi://"+getClientHost()+":1099/downloader";
			IDownload download = (IDownload) Naming.lookup(rmiAddress);
			downloads.put(getClientHost(), download);
			logger.info("Downloader has been registered " + getClientHost());
		} catch (Exception e) {
			logger.warn("Download the registration failed", e);
		}
	}
	
	private Element validateConfig(String xmlConfig) throws Exception{
		SAXBuilder builder = new SAXBuilder();
		Document document = builder.build(new StringReader(xmlConfig));
		Element root = document.getRootElement();
		//校验线程数不能小于download实例数量
		Element threadElement = root.getChild("Thread");
		if (threadElement != null){
			Element downloadControl = root.getChild("DownloadControl");
			int downloadCount = downloads.size();
			if (downloadControl != null){
				downloadCount = downloadControl.getChildren("Download").size();
			}
			int thread = Integer.parseInt(threadElement.getTextTrim());
			if (thread > downloadCount){
				throw new Exception("The number of threads cannot be less than the downloader");
			}
		}
		return root;
	}

	public void startTask(final String xmlConfig) throws RemoteException {
		TaskServer taskServer = null;
		Element root = null;
		int threadNum = 0 ;
		try{
			root = validateConfig(xmlConfig);
			final String name = root.getAttributeValue("name");
			taskServer = new TaskServer(name);
			Element queue = root.getChild("Queue");
			if (queue != null && queue.hasAttributes()){
				String queueType = queue.getAttributeValue("type");
				switch(queueType){
				case "DelayedPriorityBlockingQueue":
					int delayInMilliseconds = Integer.parseInt(queue.getTextTrim());
					taskServer.setRequestQueue(new DelayedPriorityBlockingQueue(delayInMilliseconds));
					break;
				case "DelayedBlockingQueue":
					delayInMilliseconds = Integer.parseInt(queue.getTextTrim());
					taskServer.setRequestQueue(new DelayedBlockingQueue(delayInMilliseconds));
					break;
				case "RequestPriorityBlockingQueue":
					taskServer.setRequestQueue(new RequestPriorityBlockingQueue());
					break;
				case "SimpleBlockingQueue":
					taskServer.setRequestQueue(new SimpleBlockingQueue());
					break;
				}
			}
			Element seed = root.getChild("StartContext");
			List<Element> requests = seed.getChildren("PageRequest");
			StartContext startContext = new StartContext();
			for (Element request : requests) {
				PageRequest req = startContext.createPageRequest(request.getChildText("Url"), XmlConfigPageProcessor.class);
				req.setProcessorAddress(request.getAttributeValue("processor"));
				startContext.injectSeed(req);
			}
			taskServer.setContext(startContext);
			//配置线程数和下载主机
			Element downloadControl = root.getChild("DownloadControl");
			if (downloadControl != null){
				List<Element> downloadEles = downloadControl.getChildren("Download");
				for (Element elm : downloadEles) {
					taskServer.addDownloadHost(elm.getTextTrim());
				}
			}else{
				for (String downloadHost : downloads.keySet()) {
					taskServer.addDownloadHost(downloadHost);
				}
			}
			Element thread = root.getChild("Thread");
			if (thread != null){
				threadNum = Integer.parseInt(thread.getTextTrim());
			}else{
				threadNum = taskServer.getDownloadCount();
			}
			String taskKey = PrefixInfo.TASK_PREFIX + taskServer.getTaskName() + PrefixInfo.TASK_CONFIG;
			cacheConfigXml(taskKey, xmlConfig);
			taskServer.start(threadNum);
			tasks.put(taskServer.getTaskName(), taskServer);
		}catch(Exception e){
			logger.warn("启动任务失败", e);
			throw new RemoteException(e.getMessage());
		}
		
	}
	
	public void cacheConfigXml(final String key,final String xmlConfig){
		redis.exe(new OperationJedis<Void>() {

			@Override
			public Void operation(Jedis jedis) throws Exception {
				jedis.set(key, xmlConfig);
				return null;
			}
		});
	}

	public Object getTaskPropertie(String taskName, String propertieName) throws RemoteException {
		TaskServer task = tasks.get(taskName);
		if (task != null){
			Map<String,Object> properties = task.getProperties();
			return properties.get(propertieName);
		}
		return null;
	}

	public void pushTaskRequests(String taskName, List<BasicRequest> requests) throws RemoteException {
		TaskServer task = tasks.get(taskName);
		if (task != null){
			task.pushRequests(requests);
		}
	}
	
	public void run() {
//		while(true){
//			for (Map.Entry<String,IDownload> entry: downloads.entrySet()) {
//				IDownload download = entry.getValue();
//				try {
//					NodeStatus ns = download.getStatus();
//					lastNodeStatus.put(download, ns);
//				} catch (RemoteException e) {
//					logger.warn("heartbeat check", e);
//					//移除download，通知所有task
//				}
//			}
//			weightCalculating();
//			sleep(heartCheckInterval);
//		}
	}
	
	private void sleep(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	protected void weightCalculating(){
		weights.clear();
		for (Map.Entry<IDownload, NodeStatus> entry: lastNodeStatus.entrySet()) {
			NodeStatus ns = entry.getValue();
			double rateMemory = (double)ns.getFreeMemory()/ns.getTotalMemory();
			int weight = (int) (rateMemory * 100);
			weight += ns.getCpuNum() * 2;
			weight -= ns.getActiveThread()/10;
			if (weight < 0){
				weight = 0;
			}
			weights.put(entry.getKey(),weight);
		}
	}
	
//	public Object getStartContextAttribute(String taskName, String hashCode, String attribute) {
//		TaskServer task = tasks.get(taskName);
//		if (task != null){
//			Object value = task.getRemoteDownload().getContextAttribute(hashCode, attribute);
//			return value;
//		}
//		return null;
//	}

	@Override
	public Object getMasterPropertie(String name) throws RemoteException {
		return masterProperties.get(name);
	}
	
}
