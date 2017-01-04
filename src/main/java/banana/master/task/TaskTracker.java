package banana.master.task;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
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
import banana.core.modle.TaskError;
import banana.core.protocol.Task;
import banana.core.queue.BlockingRequestQueue;
import banana.core.queue.RequestQueueBuilder;
import banana.core.request.Cookies;
import banana.core.request.HttpRequest;
import banana.core.request.RequestBuilder;
import banana.core.request.StartContext;
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
	
	private SeedQuerys seedQuerys;

	private StartContext context;

	private Filter filter = null;
	
	private BackupRunnable backupRunnable;
	
	private boolean runing = true;
	
	private Cookies initCookies;
	
	private HashSet<TaskError> errorStash = new HashSet<TaskError>();
	
	public TaskTracker(Task taskConfig) throws Exception {
		this(taskConfig, null);
	}
	
	public TaskTracker(Task taskConfig,Cookies initCookies) throws Exception {
		config = taskConfig;
		taskId = taskConfig.name + "_" + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
		context = new StartContext();
		initContext(config.seeds, config.seed_query);
		initFilter(config.filter);
		initQueue(config.queue);
		setBackup();
		initPreviousLinks(config.synchronizeLinks, config.name, config.collection);
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
				HttpRequest req = RequestBuilder.custom().setUrl(urls[i]).setProcessor(seed.processor).build();
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
				urls = new String[]{seed.download};
			}else if (seed.downloads != null){
				urls = seed.downloads;
			}
			for (int i = 0; downloads != null && i < downloads.length; i++) {
				HttpRequest req = RequestBuilder.custom().setDownload(downloads[i]).setProcessor("").build();
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
			this.seedQuerys = new SeedQuerys(config.collection, seedQuery);
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
	

	private void initPreviousLinks(boolean synchronizeLinks,String name,String collection) throws Exception {
		GridFS tracker_status = new GridFS(MasterServer.getInstance().getMongoDB(),"tracker_stat");
		GridFSDBFile file = tracker_status.findOne(name + "_" + collection + "_filter");
		if (file != null){
			byte[] filterData = SystemUtil.inputStreamToBytes(file.getInputStream());
			filter.load(filterData);
		}
		file = tracker_status.findOne(name + "_" + collection + "_context");
		if (file != null){
			byte[] contextData = SystemUtil.inputStreamToBytes(file.getInputStream());
			context.load(contextData);
		}
		if (synchronizeLinks){
			file = tracker_status.findOne(name + "_" + collection + "_links");
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
	
	private void initSeedToRequestQueue() throws Exception {
		if (!requestQueue.isEmpty()){
			return;
		}
		List<HttpRequest> seeds = context.getSeedRequests();
		for (HttpRequest req : seeds) {
			requestQueue.add(req);
		}
		if (seedQuerys != null){
			List<HttpRequest> generators = seedQuerys.query();
			for (HttpRequest req : generators) {
				requestQueue.add(req);
			}
		}
	}

	public void start() throws Exception {
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
		if (isAllWaiting() && requestQueue.isEmpty()) {
			if (seedQuerys != null && seedQuerys.canQuery()){
				synchronized (this) {
					if (requestQueue.isEmpty()){
						initSeedToRequestQueue();
						return requestQueue.poll();
					}
				}
			}else{
				new Thread(){
					public void run() {
						MasterServer.getInstance().stopTask(getTaskName());
					};
				}.start();
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
	
	public List<RemoteDownloaderTracker> getDownloads() {
		return downloads;
	}
	
	
	public void errorStash(TaskError error){
		if (!errorStash.contains(error)){
			DBCollection collection = MasterServer.getInstance().getMongoDB().getCollection("task_error");
			BasicDBObject dbObject = new BasicDBObject("error_type",error.errorType)
					.append("time", error.time)
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
		logger.info(config.name + " destory finished");
	}
}
