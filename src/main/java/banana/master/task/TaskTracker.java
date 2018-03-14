package banana.master.task;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import banana.core.ExpandHandlebars;
import banana.core.exception.DownloadException;
import banana.core.filter.Filter;
import banana.core.filter.MongoDBFilter;
import banana.core.filter.NotFilter;
import banana.core.filter.SimpleBloomFilter;
import banana.core.modle.ContextModle;
import banana.core.modle.Task;
import banana.core.modle.TaskError;
import banana.core.queue.BlockingRequestQueue;
import banana.core.queue.RequestQueueBuilder;
import banana.core.request.Cookies;
import banana.core.request.HttpRequest;
import banana.core.request.PageRequest.PageEncoding;
import banana.core.request.RequestBuilder;
import banana.core.util.SystemUtil;
import banana.master.MasterServer;

public class TaskTracker {
	
	public static final int RUN_MODE = 0;
	
	public static final int TEST_MODE = 1;
	
	public static int MODE = RUN_MODE;

	private static Logger logger = Logger.getLogger(TaskTracker.class);

	private String taskId;

	private Task config;

	private List<RemoteDownloaderTracker> downloads = new ArrayList<RemoteDownloaderTracker>();

	private BlockingRequestQueue requestQueue;
	
	private SeedQuery seedQuery;

	private TaskContextImpl context;

	private Filter filter = null;
	
	private BackupRunnable backupRunnable;
	
	private boolean runing = true;
	
	private Cookies initCookies;
	
	private HashSet<TaskError> errorStash = new HashSet<TaskError>();
	
	private StatusChecker statusChecker;
	
	public TaskTracker(Task taskConfig) throws Exception {
		this(taskConfig, null, null);
	}
	
	public TaskTracker(Task task, Map<String, Object> prepradContext) throws Exception {
		this(task, prepradContext, null);
	}
	
	public TaskTracker(Task taskConfig,Map<String,Object> prepradContext,Cookies initCookies) throws Exception {
		config = taskConfig;
		taskId = taskConfig.name + "_" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		context = new TaskContextImpl(prepradContext);
		initContext(config.seed.init.seeds, config.seed.init.seed_query);
		initFilter(config.filter);
		initQueue(config.queue);
		if (config.mode == null || !config.mode.prepared){
			initPreviousStatus(config.synchronizeLinks, config.name, config.collection);
			setBackup();
		}
		initSeedToRequestQueue();
		this.initCookies = initCookies;
		logger.info(String.format("TaskTracker %s use filter %s", taskId, filter.getClass().getName()));
		logger.info(String.format("TaskTracker %s use queue %s", taskId, requestQueue.getClass().getName()));
		logger.info(String.format("TaskTracker %s use seeds %d", taskId, requestQueue.size()));
		logger.info(String.format("TaskTracker %s use init cookies %s", taskId, initCookies != null));
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
	
	private void initContext(List<Task.Seed> seeds,Task.SeedQuery seedQuery) throws IOException{
		context.clearSeeds();//清除之前的种子
		for (Task.Seed seed : seeds) {
			String[] urls = null;
			if (seed.url != null){
				urls = new String[]{seed.url};
			}else if (seed.urls != null){
				urls = seed.urls;
			}else if (seed.url_iterator != null && !seed.url_iterator.isEmpty()){
				List<Map<String,Object>> dataUrls = new ExpandHandlebars().toFor(seed.url_iterator);
				urls = new String[dataUrls.size()];
				for (int i = 0; i < urls.length; i++) {
					urls[i] = (String) dataUrls.get(i).get("url");
				}
			}
			for (int i = 0; urls != null && i < urls.length; i++) {
				HttpRequest req = RequestBuilder.custom().setUrl(context.parseString(urls[i])).setProcessor(seed.processor).build();
				if (seed.method == null || "GET".equalsIgnoreCase(seed.method)){
					req.setMethod(HttpRequest.Method.GET);
				}else{
					req.setMethod(HttpRequest.Method.POST);
					Map<String,String> params = seed.params;
					for (Map.Entry<String, String> valuePair : params.entrySet()){
						req.putParams(valuePair.getKey(), valuePair.getValue());
					}
				}
				if (seed.headers != null){
					for (Map.Entry<String, String> valuePair : seed.headers.entrySet()) {
						req.putHeader(valuePair.getKey(), valuePair.getValue());
					}
				}
				context.injectSeed(req);
			}
			String[] downloads = null;
			if (seed.download != null){
				downloads = new String[]{seed.download};
			}else if (seed.downloads != null){
				downloads = seed.downloads;
			}
			for (int i = 0; downloads != null && i < downloads.length; i++) {
				HttpRequest req = RequestBuilder.custom().setDownload(context.parseString(downloads[i])).setProcessor(seed.processor).build();
				if (seed.method == null || "GET".equalsIgnoreCase(seed.method)){
					req.setMethod(HttpRequest.Method.GET);
				}else{
					req.setMethod(HttpRequest.Method.POST);
					Map<String,String> params = seed.params;
					for (Map.Entry<String, String> valuePair : params.entrySet()){
						req.putParams(valuePair.getKey(), valuePair.getValue());
					}
				}
				if (seed.headers != null){
					for (Map.Entry<String, String> valuePair : seed.headers.entrySet()) {
						req.putHeader(valuePair.getKey(), valuePair.getValue());
					}
				}
				context.injectSeed(req);
			}
		}
		if (seedQuery != null){
			this.seedQuery = new SeedQuery(config.collection, seedQuery);
		}
	}
	
	private void initFilter(String filter){
		this.filter = new NotFilter();
		if (filter.length() > 0) {
			switch (filter) {
			case "simple":
				this.filter = new SimpleBloomFilter();
				break;
			case "mongo":
				this.filter = new MongoDBFilter(MasterServer.getInstance().getMongoDB().getCollection(config.collection));
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
	
	private void initPreviousStatus(boolean synchronizeLinks,String name,String collection) throws Exception {
		GridFS tracker_status = new GridFS(MasterServer.getInstance().getMongoDB(),"tracker_stat");
		GridFSDBFile file = tracker_status.findOne(name + "&" + collection + "&filter");
		if (file != null){
			byte[] filterData = SystemUtil.inputStreamToBytes(file.getInputStream());
			filter.load(filterData);
		}
		file = tracker_status.findOne(name + "&" + collection + "&context");
		if (file != null){
			byte[] contextData = SystemUtil.inputStreamToBytes(file.getInputStream());
			context.load(contextData);
		}
		if (synchronizeLinks){
			file = tracker_status.findOne(name + "&" + collection + "&links");
			if (file != null){
				byte[] data = SystemUtil.inputStreamToBytes(file.getInputStream());
				requestQueue.load(new ByteArrayInputStream(data));
				logger.info(String.format("load last time requests %d", requestQueue.size()));
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

	public ContextModle getContext() {
		return context;
	}
	
	private void initSeedToRequestQueue() throws Exception {
		if (!requestQueue.isEmpty()){
			return;
		}
		List<HttpRequest> seeds = context.getSeedRequests();
		for (HttpRequest req : seeds) {
			requestQueue.add(req);
		}
		if (seedQuery != null){
			List<HttpRequest> generators = seedQuery.query();
			for (HttpRequest req : generators) {
				requestQueue.add(req);
			}
		}
	}

	public void start() throws Exception {
		if (config.condition.length() == 0 || context.parseString(config.condition).equals("true")){
			downloads = MasterServer.getInstance().elect(taskId, config.thread);
			logger.info(String.format("%s 分配了%d个Downloader", taskId, downloads.size()));
			for (RemoteDownloaderTracker rdt : downloads) {
				logger.info(String.format("%s Downloader %s Thread %d", taskId, rdt.getIp(), rdt.getWorkThread()));
			}
			if (downloads.isEmpty()) {
				throw new Exception("Not set any downloader");
			}
			for (RemoteDownloaderTracker taskDownload : downloads) {
				taskDownload.setTaskTracker(this);
				taskDownload.start(initCookies);
			}
			statusChecker = new StatusChecker();
			statusChecker.start();
		}else{
			logger.info(String.format("%s task condition is false %s", taskId, config.condition));
			MasterServer.getInstance().stopTaskById(taskId);
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
			List<RemoteDownloaderTracker> newTrackers = MasterServer.getInstance().electAgain(downloads,diffNum);
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
		logger.info(String.format("%s push request %s", taskId, request.getUrl()));
		request.recodeRequest();
		requestQueue.add(request);
	}

	public final HttpRequest pollRequest() throws Exception {
		HttpRequest req = null;
		for (int i = 0; i < 3; i++) {
			req = requestQueue.poll();
			if (req != null)
				return req;
			Thread.sleep(100);
		}
		return requestQueue.poll();
	}
	
	public synchronized boolean filterQuery(String fieldValue){
		return filter.contains(fieldValue);
	}
	
	public synchronized void addFilter(String fieldValue) {
		filter.add(fieldValue);
	}

	private boolean hasWorkingNode() {
		for (RemoteDownloaderTracker rdt : downloads) {
			if (rdt.isWorking()) {
				return true;
			}
		}
		return false;
	}

	public boolean containDownload(String ip, int port) {
		for (RemoteDownloaderTracker rdt : downloads) {
			if (rdt.getIp().equals(ip) && rdt.getPort() == port) {
				return true;
			}
		}
		return false;
	}
	
	public List<RemoteDownloaderTracker> getDownloads() {
		return downloads;
	}
	
	
	public void errorStash(TaskError error){
		if (!errorStash.contains(error)){
			DBCollection collection = MasterServer.getInstance().getMongoDB().getCollection("task_error");
			BasicDBObject dbObject = new BasicDBObject("error_type",error.errorType)
					.append("time", error.time)
					.append("method", error.method)
					.append("first_native_line_number", error.firstNativeLineNumber)
					.append("exception_class", error.exceptionClass)
					.append("exception_message", error.exceptionMessage)
					.append("runtime_context", error.runtimeContext)
					.append("taskname", error.taskname)
					.append("taskid", error.taskid);
			collection.insert(dbObject);
			errorStash.add(error);
		}
	}
	
	/**
	 * 任务销毁
	 */
	public void destoryTask() {
		if (runing == false){
			return;
		}
		runing = false;
		if (downloads != null){
			for (RemoteDownloaderTracker taskDownload : downloads) {
				try {
					taskDownload.stop();
				} catch (DownloadException e) {
					e.printStackTrace();
				}
			}
			logger.info("downloaderTracker closed");
		}
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
		logger.info(config.name + " destory finished");
	}
	
	public class StatusChecker extends Thread{
		
		private boolean finishedTask = false;

		@Override
		public void run() {
			try{
				while(runing){
					Thread.sleep(3 * 1000);
					if (requestQueue.isEmpty() && !hasWorkingNode()){
						if (seedQuery != null && seedQuery.canQuery()){
							initSeedToRequestQueue();
						}else if (!finishedTask){
							if (config.seed.after != null){
								initContext(config.seed.after.seeds, config.seed.after.seed_query);
								initSeedToRequestQueue();
								logger.info(String.format("TaskTracker %s task finished", taskId));
								logger.info(String.format("TaskTracker %s init after seeds", taskId));
							}
							finishedTask = true;
						}else{
							MasterServer.getInstance().stopTaskById(taskId);
						}
					}
				}
			}catch(Exception e){
				logger.error("", e);
			}
		}
		
	}
}
