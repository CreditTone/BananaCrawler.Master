package com.banana.master.impl;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


import org.apache.log4j.Logger;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.input.SAXBuilder;

import com.banana.common.NodeStatus;
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

public final class CrawlerMasterServer extends UnicastRemoteObject implements ICrawlerMasterServer,Runnable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(CrawlerMasterServer.class);
	
	public static final String PREFIX = "_PAGEPROCESSOR_";
	
	private static CrawlerMasterServer master = null;
	
	private long heartCheckInterval = 1000 * 10;
	
	private Map<String,TaskServer> tasks = new HashMap<String, TaskServer>();
	
	private Map<String,IDownload> downloads = new HashMap<String,IDownload>();
	
	private Map<IDownload,NodeStatus> lastNodeStatus = new HashMap<IDownload,NodeStatus>();
	
	private Map<IDownload,Integer> weights = new HashMap<IDownload,Integer>();
	
	protected CrawlerMasterServer() throws RemoteException {
		super();
	}
	
	static{
		try {
			master = new CrawlerMasterServer();
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
	
	public static CrawlerMasterServer getInstance(){
		return master;
	}

	public long getHeartCheckInterval() {
		return heartCheckInterval;
	}

	public void registerDownloadNode(String rmiArress) throws java.rmi.RemoteException {
		String[] hostAndPort = PatternUtil.getPatternGroup(Pattern.compile("rmi://([^:]+):(\\d+).*"), rmiArress);
		logger.debug(hostAndPort[1]);
		if (!downloads.containsKey(hostAndPort[1])){
			try {
				IDownload download = (IDownload) Naming.lookup(rmiArress);
				downloads.put(hostAndPort[1], download);
			} catch (Exception e) {
				logger.warn("", e);
			}
		}
	}

	public void startTask(String xmlConfig) throws RemoteException {
		TaskServer taskServer = null;
		try{
			SAXBuilder builder = new SAXBuilder();
			Document document = builder.build(xmlConfig);
			Element root = document.getRootElement();
			String name = root.getAttributeValue("name");
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
				req.setProcessorAddress(PREFIX + name + "_" + request.getAttributeValue("processor"));
				startContext.injectSeed(req);
			}
			taskServer.addStartContxt(startContext);
			
			List<Element> pageProcessors = root.getChildren("PageProcessor");
			
			
			
		}catch(Exception e){
			logger.warn("启动任务失败", e);
			throw new RemoteException(e.getMessage());
		}finally{
			if (taskServer != null){
				new Thread(taskServer).start();
			}
		}
		
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
	
	protected IDownload getIDownload(String host){
		IDownload download = downloads.get(host);
		if (download == null)
			throw new NullPointerException();
		return download;
	}
	
	public void run() {
		while(true){
			for (Map.Entry<String,IDownload> entry: downloads.entrySet()) {
				IDownload download = entry.getValue();
				try {
					NodeStatus ns = download.getStatus();
					lastNodeStatus.put(download, ns);
				} catch (RemoteException e) {
					logger.warn("heartbeat check", e);
					//移除download，通知所有task
				}
			}
			weightCalculating();
			sleep(heartCheckInterval);
		}
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
	
	public Collection<IDownload> getAllDownload(){
		return downloads.values();
	}
	
	public Object getStartContextAttribute(String taskName, String hashCode, String attribute) {
		TaskServer task = tasks.get(taskName);
		if (task != null){
			Object value = task.getRemoteDownload().getContextAttribute(hashCode, attribute);
			return value;
		}
		return null;
	}
	
}
