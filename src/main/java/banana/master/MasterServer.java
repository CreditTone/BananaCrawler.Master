package banana.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.log4j.Logger;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.mortbay.jetty.nio.SelectChannelConnector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.parser.ParserConfig;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

import banana.core.exception.CrawlerMasterException;
import banana.core.modle.CommandResponse;
import banana.core.modle.MasterConfig;
import banana.core.modle.TaskError;
import banana.core.modle.TaskStatus;
import banana.core.modle.TaskStatus.DownloaderTrackerStatus;
import banana.core.modle.TaskStatus.Stat;
import banana.core.protocol.MasterProtocol;
import banana.core.protocol.PreparedTask;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Task;
import banana.core.protocol.Task.Seed;
import banana.core.request.Cookie;
import banana.core.request.Cookies;
import banana.core.request.HttpRequest;
import banana.core.util.DateCodec;
import banana.master.task.RemoteDownloaderTracker;
import banana.master.task.TaskTimer;
import banana.master.task.TaskTracker;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public final class MasterServer implements MasterProtocol {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static Logger logger = Logger.getLogger(MasterServer.class);

	private static MasterServer master = null;

	private Server rpcServer = null;

	private org.eclipse.jetty.server.Server httpServer = null;

	private MasterConfig config;

	private List<RemoteDownload> downloads = new ArrayList<RemoteDownload>();

	private DB db;

	private Connection jdbcConnection;
	
	private TaskManager taskManager = new TaskManager();

	public MasterServer(MasterConfig config) {
		super();
		this.config = config;
		master = this;
	}

	public static final MasterServer getInstance() {
		return master;
	}

	public DB getMongoDB() {
		if (db == null) {
			MongoClient client = null;
			ServerAddress serverAddress;
			try {
				serverAddress = new ServerAddress(config.mongodb.host, config.mongodb.port);
				List<ServerAddress> seeds = new ArrayList<ServerAddress>();
				seeds.add(serverAddress);
				MongoCredential credentials = MongoCredential.createCredential(config.mongodb.username,
						config.mongodb.db, config.mongodb.password.toCharArray());
				client = new MongoClient(seeds, Arrays.asList(credentials));
				db = client.getDB(config.mongodb.db);
			} catch (Exception e) {
				logger.info("init mongodb error", e);
			}
		}
		return db;
	}

	public Connection getSqlConnection() throws SQLException {
		if (config.jdbc != null && (jdbcConnection == null || jdbcConnection.isClosed())) {
			String className = null;
			if (config.jdbc.startsWith("jdbc:mysql://")) {
				className = "com.mysql.jdbc.Driver";
			} else if (config.jdbc.contains("jdbc:sqlserver://")) {
				className = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			}
			try {
				// 加载MySql的驱动类
				Class.forName(className);
			} catch (ClassNotFoundException e) {
				System.out.println("找不到驱动程序类 ，加载驱动失败！");
				e.printStackTrace();
			}
			jdbcConnection = DriverManager.getConnection(config.jdbc);
		}
		return jdbcConnection;
	}

	public CommandResponse registerDownloadNode(String remote, int port) {
		for (RemoteDownload download : downloads) {
			if (download.getIp().equals(remote) && port == download.getPort()) {
				return new CommandResponse(false, "download " + remote + ":" + port + " registed");
			}
		}
		try {
			DownloadProtocol dp = RPC.getProxy(DownloadProtocol.class, DownloadProtocol.versionID,
					new InetSocketAddress(remote, port), new Configuration());
			RemoteDownload rm = new RemoteDownload(remote, port, dp);
			downloads.add(rm);
			logger.info("Downloader has been registered " + remote);
		} catch (Exception e) {
			logger.warn("Downloader the registration failed", e);
			return new CommandResponse(false, "Downloader the registration failed");
		}
		return new CommandResponse(true);
	}

	public List<RemoteDownload> getDownloads() {
		return downloads;
	}

	public Object getTaskPropertie(String taskId, String propertieName) {
		TaskTracker task = taskManager.getTaskTrackerById(taskId);
		if (task != null) {
			return task.getProperties(propertieName);
		}
		return null;
	}

	public CommandResponse pushTaskRequest(String taskId, HttpRequest request) {
		TaskTracker task = taskManager.getTaskTrackerById(taskId);
		if (task != null) {
			task.pushRequest(request);
		}
		return new CommandResponse(true);
	}

	public HttpRequest pollTaskRequest(String taskId) {
		TaskTracker task = taskManager.getTaskTrackerById(taskId);
		try {
			return task.pollRequest();
		} catch (Exception e) {
			logger.warn("System error", e);
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
		if (downloads.isEmpty()) {
			return new CommandResponse(false, "There is no downloader");
		}
		TaskTracker taskTracker = taskManager.getTaskTrackerByName(config.name);
		if (taskTracker != null) {
			taskTracker.updateConfig(config);
			return new CommandResponse(true);
		}
		if (config.mode != null && config.mode.timer != null) {
			if (taskManager.existTimer(config.name)){
				return new CommandResponse(false, "名字为"+config.name+"的定时器已经存在");
			}else{
				TaskTimer taskTimer = new TaskTimer(config);
				taskManager.addTaskTimer(taskTimer);
				taskTimer.start();
			}
		}else if (config.mode != null && config.mode.prepared){
			taskManager.addPreparedTask(config);
		}else{
			TaskTracker tracker = new TaskTracker(config);
			taskManager.addTaskTracker(tracker);
			tracker.start();
		}
		return new CommandResponse(true);
	}
	
	/**
	 * 选举Downloader LoadBalance
	 * 
	 * @param taskId
	 * @param threadNum
	 * @return
	 */
	public List<RemoteDownloaderTracker> elect(String taskId, int threadNum) {
		if (threadNum > 0 && threadNum <= 3) {
			return Arrays.asList(
					new RemoteDownloaderTracker(threadNum, downloads.get(new Random().nextInt(downloads.size()))));
		}
		List<RemoteDownloaderTracker> taskDownloads = new ArrayList<RemoteDownloaderTracker>();
		int[] threadNums = new int[downloads.size()];
		while (true) {
			for (int i = 0; i < threadNums.length; i++) {
				if (threadNum < 6) {
					threadNums[i] += threadNum;
					threadNum = 0;
					break;
				}
				threadNums[i] += 3;
				threadNum -= 3;
			}
			if (threadNum == 0) {
				break;
			}
		}
		for (int i = 0; i < threadNums.length; i++) {
			if (threadNums[i] > 0) {
				taskDownloads.add(new RemoteDownloaderTracker(threadNums[i], downloads.get(i)));
			} else {
				break;
			}
		}
		return taskDownloads;
	}

	public List<RemoteDownloaderTracker> electAgain(List<RemoteDownloaderTracker> rdts, int addThread)
			throws Exception {
		Map<Integer, RemoteDownloaderTracker> newRemoteDownload = null;
		int allThread = addThread;
		TaskTracker tracker = null;
		for (RemoteDownloaderTracker rdt : rdts) {
			allThread += rdt.getWorkThread();
			if (tracker == null) {
				tracker = rdt.getTaskTracker();
			}
		}
		int[] threads = new int[rdts.size()];
		for (int i = 0; i < threads.length; i++) {
			threads[i] = rdts.get(i).getWorkThread();
		}

		Random rand = new Random();
		if (downloads.size() > rdts.size()) {
			int average = allThread / rdts.size();
			int shouldNum = downloads.size();
			int shouldThread;
			// 预估平均线程数必须大于原有每台机器的线程平均数
			while ((shouldThread = allThread / shouldNum) < average) {
				shouldNum -= 1;
			}
			int shouldAddDownload = shouldNum - rdts.size();
			newRemoteDownload = new HashMap<Integer, RemoteDownloaderTracker>();
			RemoteDownload newDownload = null;
			while (shouldAddDownload > 0) {
				int index = rand.nextInt(downloads.size());
				newDownload = downloads.get(index);
				if (!newRemoteDownload.containsKey(index)
						&& !tracker.containDownload(newDownload.getIp(), newDownload.getPort())) {
					newRemoteDownload.put(index, new RemoteDownloaderTracker(shouldThread, newDownload));
					addThread -= shouldThread;
					shouldAddDownload -= 1;
				}
			}
			if (addThread < 0) {
				logger.info(String.format("allThread %d shouldNum %d shouldThread %d shouldAddDownload %d", allThread,
						shouldNum, shouldThread, shouldAddDownload));
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
		return newRemoteDownload != null ? new ArrayList<RemoteDownloaderTracker>(newRemoteDownload.values()) : null;
	}

	@Override
	public IntWritable removeBeforeResult(String collection, String taskName) throws Exception {
		WriteResult result = getMongoDB().getCollection(collection).remove(new BasicDBObject("_task_name", taskName));
		return new IntWritable(result.getN());
	}

	@Override
	public BooleanWritable filterQuery(String taskId, String... fields) {
		TaskTracker task = taskManager.getTaskTrackerById(taskId);
		if (task != null) {
			boolean result = task.filterQuery(fields);
			task.addFilter(fields);
			return new BooleanWritable(result);
		}
		return null;
	}

	@Override
	public synchronized CommandResponse stopTask(String taskname) {
		TaskTracker tracker = taskManager.getTaskTrackerByName(taskname);
		if (tracker != null) {
			tracker.destoryTask();
			taskManager.removeTaskTracker(taskname);
			return new CommandResponse(true);
		}
		return new CommandResponse(false, "not found task");
	}

	public class InjectCookiesServlet extends HttpServlet {

		@Override
		protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			String taskname = req.getParameter("taskname");
			String cookiesJson = req.getParameter("cookies");
			try {
				Cookies cookies = new Cookies();
				JSONArray arr = JSON.parseArray(cookiesJson);
				for (int i = 0; i < arr.size(); i++) {
					cookies.addCookie(JSON.parseObject(arr.getJSONObject(i).toJSONString(), Cookie.class));
				}
				injectCookies(cookies, taskname);
				resp.getWriter().write("{\"success\":true}");
			} catch (Exception e) {
				logger.warn("inject cookies", e);
				resp.getWriter().write("{\"success\":false}");
			}
		}
	}

	@Override
	public CommandResponse injectCookies(Cookies cookies, String taskname) throws Exception {
		TaskTracker task = taskManager.getTaskTrackerByName(taskname);
		if (task != null) {
			List<RemoteDownloaderTracker> downloaderTracker = task.getDownloads();
			for (RemoteDownloaderTracker rdt : downloaderTracker) {
				rdt.injectCookies(cookies);
			}
		}
		return new CommandResponse(true);
	}

	@Override
	public MasterConfig getMasterConfig() throws CrawlerMasterException {
		return config;
	}

	@Override
	public synchronized CommandResponse stopCluster() throws Exception {
		for (TaskTracker tracker : taskManager.allTaskTracker()) {
			stopTask(tracker.getTaskName());
		}
		for (TaskTimer timer : taskManager.allTaskTimer()) {
			timer.stop();
		}
		for (RemoteDownload remote : downloads) {
			remote.stopDownloader();
		}
		rpcServer.stop();
		httpServer.stop();
		rpcServer = null;
		downloads = null;
		return new CommandResponse(true);
	}

	public void start() throws Exception {
		rpcServer = new RPC.Builder(new Configuration()).setProtocol(MasterProtocol.class).setInstance(this)
				.setBindAddress("0.0.0.0").setPort(config.listen).setNumHandlers(config.handlers).build();
		rpcServer.start();
		httpServer = new org.eclipse.jetty.server.Server(config.listen + 1000);
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.addServlet(new ServletHolder(new InjectCookiesServlet()), "/injectCookies");
		context.addServlet(new ServletHolder(new StartPreparedTaskServlet()), "/startPreparedTask");
		httpServer.setHandler(context);
		httpServer.start();
	}

	@Override
	public TaskStatus taskStatus(String taskname) {
		TaskStatus status = new TaskStatus();
		status.name = taskname;
		TaskTracker tracker = taskManager.getTaskTrackerByName(taskname);
		if (tracker != null){
			status.stat = TaskStatus.Stat.Runing;
			status.id = tracker.getId();
			status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
			for (RemoteDownloaderTracker remoteTracker : tracker.getDownloads()) {
				status.downloaderTrackerStatus.add(remoteTracker.getStatus());
			}
			if (getMongoDB().collectionExists(taskname)) {
				DBCursor cursor = getMongoDB().getCollection(tracker.getConfig().collection)
						.find(new BasicDBObject("_task_name", taskname));
				status.dataCount = cursor.count();
				cursor.close();
			}
			GridFS tracker_status = new GridFS(getMongoDB(), "tracker_stat");
			GridFSDBFile file = tracker_status.findOne(taskname + "_" + tracker.getConfig().collection + "_links");
			status.requestCount = (file != null) && file.getLength() > 0 ? 1 : 0;
		}else if (taskManager.existTimer(taskname)){
			TaskTimer taskTimer = taskManager.getTaskTimerByName(taskname);
			status.stat = taskTimer.stat;
			status.id = "";
			status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
			tracker = taskTimer.tracker;
			if (status.stat == Stat.Runing){
				for (RemoteDownloaderTracker remoteTracker : tracker.getDownloads()) {
					status.downloaderTrackerStatus.add(remoteTracker.getStatus());
				}
				if (getMongoDB().collectionExists(taskname)) {
					DBCursor cursor = getMongoDB().getCollection(tracker.getConfig().collection)
							.find(new BasicDBObject("_task_name", taskname));
					status.dataCount = cursor.count();
					cursor.close();
				}
				GridFS tracker_status = new GridFS(getMongoDB(), "tracker_stat");
				GridFSDBFile file = tracker_status.findOne(taskname + "_" + tracker.getConfig().collection + "_links");
				status.requestCount = (file != null) && file.getLength() > 0 ? 1 : 0;
			}
		}else if(taskManager.existPrepared(taskname)){
			status.id = "";
			status.stat = TaskStatus.Stat.Prepared;
			status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
		}else{
			status.id = "";
			status.stat = TaskStatus.Stat.NoTask;
			status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
		}
		return status;
	}

	@Override
	public BooleanWritable verifyPassword(String password) throws Exception {
		return new BooleanWritable(password.equals(config.password));
	}
	
	public class StartPreparedTaskServlet extends HttpServlet {

		@Override
		protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			String taskname = req.getParameter("taskname");
			String seedsJson = req.getParameter("seeds");
			String cookiesJson = req.getParameter("cookies");
			try {
				PreparedTask preparedTask = new PreparedTask();
				preparedTask.name = taskname;
				if (seedsJson != null){
					JSONArray array = JSONArray.parseArray(seedsJson);
					for (int i = 0; i < array.size(); i++) {
						Seed seed = JSON.parseObject(array.getJSONObject(i).toString(), Seed.class);
						preparedTask.seeds.add(seed);
					}
				}
				if (cookiesJson != null){
					ParserConfig.global.getDerializers().put(Date.class, new DateCodec());
					JSONArray arr = JSON.parseArray(cookiesJson);
					for (int i= 0 ; i < arr.size() ; i++){
						Cookie cook = JSON.parseObject(arr.getJSONObject(i).toJSONString(), Cookie.class);
						preparedTask.cookies.add(cook);
					}
				}
				CommandResponse response = startPreparedTask(preparedTask);
				if (response.success){
					resp.getWriter().write("{\"success\":true}");
				}else{
					resp.getWriter().write("{\"success\":false,\"msg\":\""+response.error+"\"}");
				}
			} catch (Exception e) {
				logger.warn("start prepared task ", e);
				resp.getWriter().write("{\"success\":false}");
			}
		}
	}

	@Override
	public CommandResponse startPreparedTask(PreparedTask config) throws Exception {
		Task task = taskManager.getPreparedTaskByName(config.name);
		if (task != null){
			if (!config.seeds.isEmpty()){
				task.seeds = config.seeds;
			}
			TaskTracker tracker;
			if (!config.cookies.isEmpty()){
				Cookies cookies = new Cookies(config.cookies);
				tracker = new TaskTracker(task, cookies);
			}else{
				tracker = new TaskTracker(task);
			}
			taskManager.addTaskTracker(tracker);
			tracker.start();
		}else{
			return new CommandResponse(false, "prepared task not exists");
		}
		return new CommandResponse(true);
	}

	@Override
	public void errorStash(String taskId, TaskError taskError) throws Exception {
		TaskTracker taskTracker = taskManager.getTaskTrackerById(taskId);
		if (taskTracker != null){
			taskTracker.errorStash(taskError);
		}
	}

}
