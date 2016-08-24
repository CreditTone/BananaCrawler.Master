package com.banana.master.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import com.banana.master.RemoteDownload;
import com.banana.master.task.RemoteDownloaderTracker;
import com.banana.master.task.TaskTracker;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;

import banana.core.JedisOperator;
import banana.core.PropertiesNamespace;
import banana.core.exception.CrawlerMasterException;
import banana.core.exception.DownloadException;
import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Task;
import banana.core.request.BasicRequest;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest;
import banana.core.request.StartContext;
import redis.clients.jedis.Jedis;

public final class CrawlerMasterServer implements CrawlerMasterProtocol {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(CrawlerMasterServer.class);
	
	private static CrawlerMasterServer master = null;
	
	private Map<String,Text> masterProperties = new HashMap<String,Text>();
	
	private Map<String,TaskTracker> tasks = new HashMap<String, TaskTracker>();
	
	private List<RemoteDownload> downloads = new ArrayList<RemoteDownload>();
	
	private DB db;
	
	public CrawlerMasterServer() throws RemoteException {
		super();
		master = this;
	}
	
	public void init() throws NumberFormatException, UnknownHostException{
//			JedisOperator jedisOperator = JedisOperator.newInstance(redisHost, redisPort);
		String mongoAddress = masterProperties.get("MONGO").toString();
		String[] split = mongoAddress.split(",");
		MongoClient client = null;
		ServerAddress serverAddress = new ServerAddress(split[0], Integer.parseInt(split[1]));
		List<ServerAddress> seeds = new ArrayList<ServerAddress>();
		seeds.add(serverAddress);
		String userName = split[3];
		String dataBase = split[2];
		String password = split[4];
		MongoCredential credentials = MongoCredential.createCredential(userName, dataBase,
				password.toCharArray());
		client = new MongoClient(seeds, Arrays.asList(credentials));
		db = client.getDB(split[2]);
	}
	
	public static final CrawlerMasterServer getInstance(){
		return master;
	}


	public void registerDownloadNode(String remote,int port) throws CrawlerMasterException {
		for (RemoteDownload download : downloads) {
			if(download.getIp().equals(remote) && port == download.getPort()){
				return;
			}
		}
		try {
			DownloadProtocol dp = RPC.getProxy(DownloadProtocol.class, DownloadProtocol.versionID, new InetSocketAddress(remote,port),new Configuration());
			RemoteDownload rm = new RemoteDownload(remote,port);
			rm.setDownloadProtocol(dp);
			downloads.add(rm);
			logger.info("Downloader has been registered " + remote);
		} catch (Exception e) {
			logger.warn("Downloader the registration failed", e);
		}
	}
	
	public List<RemoteDownload> getDownloads(){
		return downloads;
	}
	

	public Object getTaskPropertie(String taskId, String propertieName) {
		TaskTracker task = tasks.get(taskId);
		if (task != null){
			return task.getProperties(propertieName);
		}
		return null;
	}

	public void pushTaskRequest(String taskId, HttpRequest request) {
		TaskTracker task = tasks.get(taskId);
		if (task != null){
			task.pushRequest(request);
		}
	}
	
	public HttpRequest pollTaskRequest(String taskId) {
		TaskTracker task = tasks.get(taskId);
		try {
			return task.pollRequest();
		} catch (InterruptedException e) {
			logger.warn("System error",e);
		}
		return null;
	}
	
	public void setMasterPropertie(String name,String value){
		masterProperties.put(name, new Text(value));
	}
	
	@Override
	public Text getMasterPropertie(String name) {
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

	public void submitTask(Task config) throws Exception {
		if (downloads.isEmpty()){
			throw new RuntimeException("There is no downloader");
		}
		if (isResubmit(config)){
			return;
		}
		TaskTracker tracker = new TaskTracker(config);
		StartContext context = new StartContext();
		for (Task.Seed seed : config.seeds) {
			PageRequest req = context.createPageRequest(seed.getUrl(), seed.getProcessor());
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
		tracker.setContext(context);
		tracker.start(config.thread);
		tasks.put(tracker.getId(), tracker);
	}
	
	private boolean isResubmit(Task config) throws Exception {
		for (TaskTracker tracker : tasks.values()) {
			if (config.name.equals(tracker.getTaskName())){
				tracker.updateConfig(config);
				return true;
			}
		}
		return false;
	}
	
	

	/**
	 * 选举Downloader LoadBalance
	 * @param taskId
	 * @param threadNum
	 * @return
	 */
	public List<RemoteDownloaderTracker> elect(String taskId,int threadNum) {
		if (threadNum > 0 && threadNum <= 3){
			return Arrays.asList(new RemoteDownloaderTracker(threadNum, downloads.get(new Random().nextInt(downloads.size()))));
		}
		List<RemoteDownloaderTracker> taskDownloads = new ArrayList<RemoteDownloaderTracker>();
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
				taskDownloads.add(new RemoteDownloaderTracker(threadNums[i], downloads.get(i)));
			}else{
				break;
			}
		}
		return taskDownloads;
	}
	
	
	public List<RemoteDownloaderTracker> electAgain(List<RemoteDownloaderTracker> rdts,int addThread) throws Exception {
		Map<Integer,RemoteDownloaderTracker> newRemoteDownload = null;
		int allThread = addThread;
		TaskTracker tracker = null;
		for (RemoteDownloaderTracker rdt : rdts) {
			allThread += rdt.getWorkThread();
			if (tracker == null){
				tracker = rdt.getTaskTracker();
			}
		}
		int[] threads = new int[rdts.size()];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = rdts.get(i).getWorkThread();
		}
		
		Random rand = new Random();
		if (downloads.size() > rdts.size()){
			int average = allThread / rdts.size();
			int shouldNum = downloads.size();
			int shouldThread; 
			//预估平均线程数必须大于原有每台机器的线程平均数
			while((shouldThread = allThread / shouldNum) < average){
				shouldNum -= 1;
			}
			int shouldAddDownload = shouldNum - rdts.size();
			newRemoteDownload = new HashMap<Integer,RemoteDownloaderTracker>();
			RemoteDownload newDownload = null;
			while(shouldAddDownload > 0){
				int index = rand.nextInt(downloads.size());
				newDownload = downloads.get(index);
				if (!newRemoteDownload.containsKey(index) && !tracker.containDownload(newDownload.getIp(), newDownload.getPort())){
					newRemoteDownload.put(index,new RemoteDownloaderTracker(shouldThread, newDownload));
					addThread -= shouldThread;
					shouldAddDownload -= 1;
				}
			}
			if (addThread < 0){
				logger.info(String.format("allThread %d shouldNum %d shouldThread %d shouldAddDownload %d", allThread, shouldNum ,shouldThread, shouldAddDownload));
				throw new Exception("elect error");
			}
		}
		for (int j = 0; (j < threads.length && addThread > 0); j++) {
			threads[j] += 1;
			addThread -= 1;
		}
		for (int i = 0; i < threads.length; i++) {
			rdts.get(i).updateConfig(threads[i]);
		}
		return newRemoteDownload != null?new ArrayList<RemoteDownloaderTracker>(newRemoteDownload.values()):null;
	}

	@Override
	public BooleanWritable existTask(String taskName) {
		for (TaskTracker tracker : tasks.values()) {
			if (taskName.equals(tracker.getTaskName())){
				return new BooleanWritable(true);
			}
		}
		return new BooleanWritable(false);
	}
	
	public void removeTask(String taskId){
		tasks.remove(taskId);
	}
	
	@Override
	public IntWritable removeBeforeResult(String collection, String taskName) throws Exception {
		WriteResult result = db.getCollection(collection).remove(new BasicDBObject("_task_name", taskName));
		return new IntWritable(result.getN());
	}

	@Override
	public BooleanWritable dataExists(String collection,String taskName) {
		if (db.collectionExists(collection)){
			DBObject obj = db.getCollection(collection).findOne(new BasicDBObject("_task_name", taskName));
			return new BooleanWritable(obj != null);
		}
		return new BooleanWritable(false);
	}
	
}
