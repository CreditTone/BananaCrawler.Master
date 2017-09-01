package banana.master;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import banana.core.modle.BasicWritable;
import banana.core.modle.CommandResponse;
import banana.core.modle.ContextModle;
import banana.core.modle.MasterConfig;
import banana.core.modle.PreparedTask;
import banana.core.modle.Task;
import banana.core.modle.TaskError;
import banana.core.modle.TaskStatus;
import banana.core.modle.Task.Seed;
import banana.core.modle.TaskStatus.DownloaderTrackerStatus;
import banana.core.protocol.MasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.request.Cookie;
import banana.core.request.Cookies;
import banana.core.request.HttpRequest;
import banana.core.util.DateCodec;
import banana.master.serlvet.DoulabaoCookieServlet;
import banana.master.serlvet.GjpRsaServlet;
import banana.master.serlvet.TaobaoConvertLoanData;
import banana.master.serlvet.TaobaoCreateTimeServlet;
import banana.master.serlvet.TaobaoShopRateServlet;
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
	
	private ContextModle globalContext = new AutoOverdueContext(24, 1000 * 3600);

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
	
	public ContextModle getGlobalContext() {
		return globalContext;
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

	public CommandResponse pushTaskRequest(String taskId, HttpRequest request) {
		TaskTracker task = taskManager.getTaskTrackerById(taskId);
		if (task != null) {
			task.pushRequest(request);
		}
		return new CommandResponse(true, task.getId());
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
			logger.info("prepared task " + config.name);
		}else{
			taskManager.verify(config);
			TaskTracker tracker = new TaskTracker(config);
			taskManager.addTaskTracker(tracker);
			tracker.start();
			return new CommandResponse(true, tracker.getId());
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
	public synchronized CommandResponse stopTaskById(String taskid) {
		TaskTracker tracker = taskManager.getTaskTrackerById(taskid);
		if (tracker != null) {
			tracker.destoryTask();
			taskManager.removeTaskTrackerById(taskid);
			return new CommandResponse(true, taskid);
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
		Collection<TaskTracker> trackers = taskManager.getTaskTrackerByName(taskname);
		for (TaskTracker tracker : trackers) {
			List<RemoteDownloaderTracker> downloaderTracker = tracker.getDownloads();
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
			stopTaskById(tracker.getTaskName());
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
		return new CommandResponse(true,"");
	}

	public void start() throws Exception {
		rpcServer = new RPC.Builder(new Configuration()).setProtocol(MasterProtocol.class).setInstance(this)
				.setBindAddress("0.0.0.0").setPort(config.listen).setNumHandlers(config.handlers).build();
		rpcServer.start();
		httpServer = new org.eclipse.jetty.server.Server(config.listen + 1000);
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.addServlet(new ServletHolder(new InjectCookiesServlet()), "/injectCookies");
		context.addServlet(new ServletHolder(new StartPreparedTaskServlet()), "/startPreparedTask");
		context.addServlet(new ServletHolder(new GjpRsaServlet()), "/service/gjprsa");
		context.addServlet(new ServletHolder(new TaobaoShopRateServlet()), "/service/taobaoShopRate");
		context.addServlet(new ServletHolder(new TaobaoCreateTimeServlet()), "/service/taobaoCreatetime");
		context.addServlet(new ServletHolder(new TaobaoConvertLoanData()), "/service/taobaoConvertLoanData");
		context.addServlet(new ServletHolder(new DoulabaoCookieServlet()), "/service/doulabaoCookie");
		httpServer.setHandler(context);
		httpServer.start();
	}

	@Override
	public TaskStatus getTaskStatusById(String taskid) {
		TaskTracker tracker = taskManager.getTaskTrackerById(taskid);
		if (tracker != null){
			TaskStatus status = new TaskStatus();
			status.name = tracker.getTaskName();
			status.stat = TaskStatus.Stat.Runing;
			status.id = tracker.getId();
			status.downloaderTrackerStatus = new ArrayList<DownloaderTrackerStatus>();
			for (RemoteDownloaderTracker remoteTracker : tracker.getDownloads()) {
				status.downloaderTrackerStatus.add(remoteTracker.getStatus());
			}
			if (getMongoDB().collectionExists(status.name)) {
				DBCursor cursor = getMongoDB().getCollection(tracker.getConfig().collection)
						.find(new BasicDBObject("_task_name", status.name));
				status.dataCount = cursor.count();
				cursor.close();
			}
			GridFS tracker_status = new GridFS(getMongoDB(), "tracker_stat");
			GridFSDBFile file = tracker_status.findOne(status.name + "_" + tracker.getConfig().collection + "_links");
			status.requestCount = (file != null) && file.getLength() > 0 ? 1 : 0;
			return status;
		}
		return null;
	}

	@Override
	public BooleanWritable verifyPassword(String password) throws Exception {
		return new BooleanWritable(password.equals(config.password));
	}
	
	public class StartPreparedTaskServlet extends HttpServlet {
		
		public static final String KEY_TASKNAME = "taskname";
		
		public static final String KEY_SEEDS = "seeds";
		
		public static final String KEY_COOKIES = "cookies";

		@Override
		protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
			String taskname = req.getParameter(KEY_TASKNAME);
			String seedsJson = req.getParameter(KEY_SEEDS);
			String cookiesJson = req.getParameter(KEY_COOKIES);
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
				Enumeration<String> parameterNames = req.getParameterNames();
				while(parameterNames.hasMoreElements()){
					String name  = parameterNames.nextElement();
					if (name.equals(KEY_TASKNAME) || name.equals(KEY_SEEDS) || name.equals(KEY_COOKIES)){
						continue;
					}
					preparedTask.taskContext.put(name, req.getParameter(name));
				}
				CommandResponse response = startPreparedTask(preparedTask);
				resp.getWriter().write(JSON.toJSONString(response));
			} catch (Exception e) {
				logger.warn("start prepared task ", e);
				resp.getWriter().write("{\"success\":false,\"msg\":\""+e.getMessage()+"\"}");
			}
		}
	}

	@Override
	public CommandResponse startPreparedTask(PreparedTask config) throws Exception {
		Task task = taskManager.getPreparedTaskByName(config.name);
		if (task == null){
			return new CommandResponse(false, "prepared task not exists");
		}
		task = (Task) task.clone();
		if (!config.seeds.isEmpty()){
			task.seed.init.seeds = config.seeds;
		}
		TaskTracker tracker;
		if (!config.cookies.isEmpty()){
			Cookies cookies = new Cookies(config.cookies);
			tracker = new TaskTracker( task, config.taskContext, cookies);
		}else{
			tracker = new TaskTracker( task, config.taskContext);
		}
		taskManager.addTaskTracker(tracker);
		tracker.start();
		return new CommandResponse(true, tracker.getId());
	}

	@Override
	public void errorStash(String taskId, TaskError taskError) throws Exception {
		TaskTracker taskTracker = taskManager.getTaskTrackerById(taskId);
		if (taskTracker != null){
			taskTracker.errorStash(taskError);
		}
	}

	@Override
	public BasicWritable getTaskContextAttribute(String taskid, String attribute) throws Exception {
		TaskTracker taskTracker = taskManager.getTaskTrackerById(taskid);
		if (taskTracker != null){
			Object value = taskTracker.getContext().get(attribute);
			if (value != null){
				return new BasicWritable(value);
			}
		}
		return null;
	}

	@Override
	public void putTaskContextAttribute(String taskid, String attribute, BasicWritable value) throws Exception {
		TaskTracker taskTracker = taskManager.getTaskTrackerById(taskid);
		if (taskTracker != null){
			taskTracker.getContext().put(attribute, value.getValue());
		}
	}

	@Override
	public void putGlobalContext(String attribute, BasicWritable value) throws Exception {
		globalContext.put(attribute, value.getValue());
	}
	
}
