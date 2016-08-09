package com.banana.master.impl;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.shell.CopyCommands.Get;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import banana.standalone.common.JedisOperator;
import banana.standalone.common.PrefixInfo;
import banana.standalone.common.PropertiesNamespace;
import banana.standalone.common.JedisOperator.Command;
import banana.standalone.common.protocol.CrawlerMasterProtocol;
import banana.standalone.common.protocol.DownloadProtocol;
import banana.standalone.exception.CrawlerMasterException;
import banana.standalone.queue.BlockingRequestQueue;
import banana.standalone.queue.RedisRequestBlockingQueue;
import banana.standalone.request.BasicRequest;
import redis.clients.jedis.Jedis;

public final class CrawlerMasterServer implements CrawlerMasterProtocol {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(CrawlerMasterServer.class);
	
	private static CrawlerMasterServer master = null;
	
	private Map<String,Object> masterProperties = new HashMap<String,Object>();
	
	private Map<String,TaskTracker> tasks = new HashMap<String, TaskTracker>();
	
	private Map<String,DownloadProtocol> downloads = new HashMap<String,DownloadProtocol>();
	
	
	private JedisOperator redis;
	
	protected CrawlerMasterServer() throws RemoteException {
		super();
	}
	
	public static void init(final String redisHost,final int redisPort){
			JedisOperator jedisOperator = JedisOperator.newInstance(redisHost, redisPort);
			Boolean success = jedisOperator.exe(new JedisOperator.Command<Boolean>() {

				@Override
				public Boolean operation(Jedis jedis) throws Exception {
					jedis.setex("TestRedisKey", 1, "TestRedisValue");
					return true;
				}

				@Override
				protected void exceptionOccurs(Exception e) {
					logger.warn("请给Master配置一个Redis服务");
				}
			});
			if (success != null && success){
				try {
					master = new CrawlerMasterServer();
					master.redis = jedisOperator;
					master.masterProperties.put(PropertiesNamespace.Master.REDIS_HOST, redisHost);
					master.masterProperties.put(PropertiesNamespace.Master.REDIS_PORT, redisPort);
				} catch (RemoteException e) {
					logger.warn("Initialize the master service failure",e);
					master = null;
				}
			}
	}
	
	public static CrawlerMasterServer getInstance(){
		return master;
	}


	public void registerDownloadNode(String remote) throws CrawlerMasterException {
		try {
			DownloadProtocol dp = RPC.getProxy(DownloadProtocol.class, DownloadProtocol.versionID, new InetSocketAddress(remote,8787),new Configuration());
			downloads.put(remote, dp);
			logger.info("Downloader has been registered " + remote);
		} catch (Exception e) {
			logger.warn("Downloader the registration failed", e);
		}
	}
	


//	public void startTask(final String xmlConfig) throws RemoteException {
//		TaskTracker taskServer = null;
//		try{
//			XmlConfig config = XmlConfig.loadXmlConfig(xmlConfig);
//			taskServer = new TaskTracker(config.getName());
//			Class queueCls = Class.forName(config.getQueueClassName());
//			BlockingRequestQueue queue = null;
//			if (config.getDelayInMilliseconds() != -1){
//				Constructor queueConstructor = queueCls.getConstructor(int.class);
//				queue = (BlockingRequestQueue) queueConstructor.newInstance(config.getDelayInMilliseconds());
//			}else if (queueCls.equals(RedisRequestBlockingQueue.class)){
//				Constructor queueConstructor = queueCls.getConstructor(String.class,int.class);
//				String redisHost = (String) master.masterProperties.get(PropertiesNamespace.Master.REDIS_HOST);
//				int redisPort = (int) master.masterProperties.get(PropertiesNamespace.Master.REDIS_PORT);
//				queue = (BlockingRequestQueue) queueConstructor.newInstance(redisHost,redisPort);
//			}else{
//				queue = (BlockingRequestQueue) queueCls.newInstance();
//			}
//			taskServer.setRequestQueue(queue);
//			taskServer.setContext(config.getStartContext());
//			if (config.getDownloadHosts() != null){
//				for (String host : config.getDownloadHosts()) {
//					taskServer.addDownloadHost(host);
//				}
//			}else{
//				for (String downloadHost : downloads.keySet()) {
//					taskServer.addDownloadHost(downloadHost);
//				}
//			}
//			String taskKey = PrefixInfo.TASK_PREFIX + taskServer.getTaskName() + PrefixInfo.TASK_CONFIG;
//			cacheConfigXml(taskKey, xmlConfig);
//			if (config.getThread() != null){
//				taskServer.start(config.getThread());
//			}else{
//				taskServer.start(taskServer.getDownloadCount());
//			}
//			tasks.put(taskServer.getTaskName(), taskServer);
//		}catch(Exception e){
//			logger.warn("启动任务失败", e);
//			throw new RemoteException(e.getMessage());
//		}
//	}
//	
//	public void cacheConfigXml(final String key,final String xmlConfig){
//		redis.exe(new Command<Void>() {
//
//			@Override
//			public Void operation(Jedis jedis) throws Exception {
//				jedis.set(key, xmlConfig);
//				return null;
//			}
//		});
//	}

	public Object getTaskPropertie(String taskName, String propertieName) throws RemoteException {
		TaskTracker task = tasks.get(taskName);
		if (task != null){
			Map<String,Object> properties = task.getProperties();
			return properties.get(propertieName);
		}
		return null;
	}

	public void pushTaskRequests(String taskName, List<BasicRequest> requests) throws RemoteException {
		TaskTracker task = tasks.get(taskName);
		if (task != null){
			task.pushRequests(requests);
		}
	}
	
	public List<BasicRequest> pollTaskRequests(String taskName,int fetchsize) throws RemoteException {
		TaskTracker task = tasks.get(taskName);
		try {
			return task.pollRequest(fetchsize);
		} catch (InterruptedException e) {
			logger.warn("System error",e);
			RemoteException re = new RemoteException();
			re.addSuppressed(e.fillInStackTrace());
			throw re;
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
	
	public Map<String,IDownload> getDownloads(){
		return this.downloads;
	}
	
}
