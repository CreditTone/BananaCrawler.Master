package banana.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import banana.core.exception.CrawlerMasterException;
import banana.core.modle.CommandResponse;
import banana.core.modle.MasterConfig;
import banana.core.modle.TaskStatus;
import banana.core.modle.TaskStatus.DownloaderTrackerStatus;
import banana.core.protocol.MasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Task;
import banana.core.request.Cookies;
import banana.core.request.HttpRequest;
import banana.master.task.RemoteDownloaderTracker;
import banana.master.task.TaskTimer;
import banana.master.task.TaskTracker;

public final class MasterServer implements MasterProtocol {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(MasterServer.class);
	
	private static MasterServer master = null;
	
	private Server rpcServer = null;
	
	private MasterConfig config;
	
	public Map<String,TaskTracker> tasks = new HashMap<String, TaskTracker>();
	
	private List<TaskTimer> timers = new ArrayList<TaskTimer>();
	
	private List<RemoteDownload> downloads = new ArrayList<RemoteDownload>();
	
	private DB db;
	
	private Connection jdbcConnection;
	
	public MasterServer(MasterConfig config){
		super();
		this.config = config;
		master = this;
	}
	
	public static final MasterServer getInstance(){
		return master;
	}
	
	public DB getMongoDB(){
		if (db == null){
			MongoClient client = null;
			ServerAddress serverAddress;
			try {
				serverAddress = new ServerAddress(config.mongodb.host, config.mongodb.port);
				List<ServerAddress> seeds = new ArrayList<ServerAddress>();
				seeds.add(serverAddress);
				MongoCredential credentials = MongoCredential.createCredential(config.mongodb.username, config.mongodb.db,
						config.mongodb.password.toCharArray());
				client = new MongoClient(seeds, Arrays.asList(credentials));
				db = client.getDB(config.mongodb.db);
			} catch (Exception e) {
				logger.info("init mongodb error", e);
			}
		}
		return db;
	}
	
	public Connection getSqlConnection() throws SQLException{
		if (config.jdbc != null && (jdbcConnection == null || jdbcConnection.isClosed())){
			String className = null;
			if (config.jdbc.startsWith("jdbc:mysql://")){
				className = "com.mysql.jdbc.Driver";
			}else if (config.jdbc.contains("jdbc:sqlserver://")){
				className = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			}
			try{   
			    //加载MySql的驱动类   
			    Class.forName(className) ;   
			}catch(ClassNotFoundException e){   
			   	System.out.println("找不到驱动程序类 ，加载驱动失败！");   
			    e.printStackTrace();
			}
			jdbcConnection = DriverManager.getConnection(config.jdbc);
		}
		return jdbcConnection;
	}
	
	public CommandResponse registerDownloadNode(String remote,int port) {
		for (RemoteDownload download : downloads) {
			if(download.getIp().equals(remote) && port == download.getPort()){
				return new CommandResponse(false,"download "+remote+":"+port+" registed");
			}
		}
		try {
			DownloadProtocol dp = RPC.getProxy(DownloadProtocol.class, DownloadProtocol.versionID, new InetSocketAddress(remote,port),new Configuration());
			RemoteDownload rm = new RemoteDownload(remote,port,dp);
			downloads.add(rm);
			logger.info("Downloader has been registered " + remote);
		} catch (Exception e) {
			logger.warn("Downloader the registration failed", e);
			return new CommandResponse(false,"Downloader the registration failed");
		}
		return new CommandResponse(true);
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

	public CommandResponse pushTaskRequest(String taskId, HttpRequest request) {
		TaskTracker task = tasks.get(taskId);
		if (task != null){
			task.pushRequest(request);
		}
		return new CommandResponse(true);
	}
	
	public HttpRequest pollTaskRequest(String taskId) {
		TaskTracker task = tasks.get(taskId);
		try {
			return task.pollRequest();
		} catch (Exception e) {
			logger.warn("System error",e);
		}
		return null;
	}
	
	@Override
	public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
		return MasterProtocol.versionID;
	}

	@Override
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash)
			throws IOException {
		return new ProtocolSignature(versionID, null);
	}

	public synchronized CommandResponse submitTask(Task config) throws Exception {
		if (downloads.isEmpty()){
			return new CommandResponse(false, "There is no downloader");
		}
		TaskTracker taskTracker = getTaskTrackerByName(config.name);
		if (taskTracker != null){
			taskTracker.updateConfig(config);
			return new CommandResponse(true);
		}
		if (config.timer != null){
			TaskTimer taskTimer = new TaskTimer(config);
			Iterator<TaskTimer> iter = timers.iterator();
			while(iter.hasNext()){
				TaskTimer timer = iter.next();
				if (timer.cfg.name.equals(config.name)){
					timers.remove(timer);
					timer.stop();
				}
			}
			timers.add(taskTimer);
			taskTimer.start();
		}else{
			TaskTracker tracker = new TaskTracker(config);
			tasks.put(tracker.getId(), tracker);
			tracker.start();
		}
		return new CommandResponse(true);
	}
	
	private TaskTracker getTaskTrackerByName(String name) throws Exception {
		for (TaskTracker tracker : tasks.values()) {
			if (name.equals(tracker.getTaskName())){
				return tracker;
			}
		}
		return null;
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
	public IntWritable removeBeforeResult(String collection, String taskName) throws Exception {
		WriteResult result = getMongoDB().getCollection(collection).remove(new BasicDBObject("_task_name", taskName));
		return new IntWritable(result.getN());
	}

	@Override
	public BooleanWritable filterQuery(String taskId, String... fields) {
		TaskTracker task = tasks.get(taskId);
		if (task != null){
			boolean result = task.filterQuery(fields);
			task.addFilter(fields);
			return new BooleanWritable(result);
		}
		return null;
	}

	@Override
	public synchronized  CommandResponse stopTask(String taskname){
		for (TaskTracker tracker : tasks.values()) {
			if (taskname.equals(tracker.getTaskName())){
				tracker.destoryTask();
				tasks.remove(tracker.getTaskName());
				return new CommandResponse(true);
			}
		}
		return new CommandResponse(false,"not found task");
	}

	@Override
	public CommandResponse injectCookies(Cookies cookies, String taskId) throws Exception {
		TaskTracker task = tasks.get(taskId);
		if (task != null){
			
		}
		return null;
	}

	@Override
	public MasterConfig getMasterConfig() throws CrawlerMasterException {
		return config;
	}

	@Override
	public synchronized CommandResponse stopCluster() throws Exception {
		for (TaskTracker tracker : tasks.values()) {
			stopTask(tracker.getTaskName());
		}
		for (RemoteDownload remote : downloads){
			remote.stopDownloader();
		}
		rpcServer.stop();
		rpcServer = null;
		downloads = null;
		return new CommandResponse(true);
	}
	
	public void start() throws HadoopIllegalArgumentException, IOException{
		rpcServer = new RPC.Builder(new Configuration()).setProtocol(MasterProtocol.class)
				.setInstance(this).setBindAddress("0.0.0.0").setPort(config.listen).setNumHandlers(config.handlers)
				.build();
		rpcServer.start();
	}

	@Override
	public TaskStatus taskStatus(Task taskconfig) {
		TaskStatus status = new TaskStatus();
		status.name = taskconfig.name;
		for (TaskTracker tracker : tasks.values()) {
			if (status.name.equals(tracker.getTaskName())){
				status.stat = TaskStatus.Stat.Runing;
				status.id = tracker.getId();
				status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
				for (RemoteDownloaderTracker remoteTracker : tracker.getDownloads()) {
					status.downloaderTrackerStatus.add(remoteTracker.getStatus());
				}
				break;
			}
		}
		if (status.id == null){
			for (TaskTimer taskTimer : timers) {
				if (status.name.equals(taskTimer.cfg.name)){
					status.stat = TaskStatus.Stat.Timing;
					status.id = "";
					status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
					break;
				}
			}
		}
		if (status.id == null){
			status.id = "";
			status.stat = TaskStatus.Stat.NoTask;
			status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
		}
		if (getMongoDB().collectionExists(taskconfig.collection)){
			DBCursor cursor = getMongoDB().getCollection(taskconfig.collection).find(new BasicDBObject("_task_name", taskconfig.name));
			status.dataCount = cursor.count();
			cursor.close();
		}
		GridFS tracker_status = new GridFS(getMongoDB(),"tracker_stat");
		GridFSDBFile file = tracker_status.findOne(taskconfig.name + "_" + taskconfig.collection + "_links");
		status.requestCount = (file!=null) && file.getLength() > 0 ? 1 : 0;
		return status;
	}

	@Override
	public BooleanWritable verifyPassword(String password) throws Exception {
		return new BooleanWritable(password.equals(config.password));
	}
	
}