package com.banana.master.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import com.banana.master.RemoteDownload;
import com.banana.master.task.TaskDownloader;
import com.banana.master.task.TaskTracker;

import banana.core.JedisOperator;
import banana.core.PropertiesNamespace;
import banana.core.exception.CrawlerMasterException;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.request.BasicRequest;
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
	
	private List<RemoteDownload> downloads = new ArrayList<RemoteDownload>();
	
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
	
	public static final CrawlerMasterServer getInstance(){
		return master;
	}


	public void registerDownloadNode(String remote) throws CrawlerMasterException {
		try {
			DownloadProtocol dp = RPC.getProxy(DownloadProtocol.class, DownloadProtocol.versionID, new InetSocketAddress(remote,8787),new Configuration());
			RemoteDownload rm = new RemoteDownload();
			rm.setDownloadProtocol(dp);
			rm.setIp(remote);
			downloads.add(rm);
			logger.info("Downloader has been registered " + remote);
		} catch (Exception e) {
			logger.warn("Downloader the registration failed", e);
		}
	}
	

	public Object getTaskPropertie(String taskName, String propertieName) {
		TaskTracker task = tasks.get(taskName);
		if (task != null){
			return task.getProperties(propertieName);
		}
		return null;
	}

	public void pushTaskRequests(String taskName, List<BasicRequest> requests) {
		TaskTracker task = tasks.get(taskName);
		if (task != null){
			task.pushRequests(requests);
		}
	}
	
	public List<BasicRequest> pollTaskRequests(String taskName,int fetchsize) {
		TaskTracker task = tasks.get(taskName);
		try {
			return task.pollRequest(fetchsize);
		} catch (InterruptedException e) {
			logger.warn("System error",e);
		}
		return null;
	}
	
	private void sleep(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public Object getMasterPropertie(String name) {
		return masterProperties.get(name);
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		return CrawlerMasterProtocol.versionID;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		return new ProtocolSignature(versionID, null);
	}

	@Override
	public void startTask(String taskConfig) throws CrawlerMasterException {
	}
	
	/**
	 * 选举Downloader LoadBalance
	 * @param taskId
	 * @param threadNum
	 * @return
	 */
	public List<TaskDownloader> elect(String taskId,int threadNum) {
		if (threadNum > 0 && threadNum <= 3){
			return Arrays.asList(new TaskDownloader(threadNum, downloads.get(new Random().nextInt(downloads.size()))));
		}
		List<TaskDownloader> taskDownloads = new ArrayList<TaskDownloader>();
		int[] threadNums = new int[downloads.size()];
		while(true){
			for (int i = 0; i < threadNums.length; i++) {
				if (threadNum < 6){
					threadNums[i] += threadNum;
					threadNum = 0;
					break;
				}
				threadNums[i] += 3;
				threadNum -= 3;
			}
			if (threadNum == 0){
				break;
			}
		}
		for (int i = 0; i < threadNums.length; i++) {
			if (threadNums[i] > 0){
				taskDownloads.add(new TaskDownloader(threadNums[i], downloads.get(i)));
			}else{
				break;
			}
		}
		return taskDownloads;
	}
	
}
