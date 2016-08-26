package banana.master.task;

import java.io.Closeable;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

import banana.core.filter.Filter;
import banana.core.protocol.Task;
import banana.core.request.StartContext;
import banana.master.impl.CrawlerMasterServer;

public class BackupRunnable extends TimerTask implements Closeable {
	
	private Task config;
	
	private Timer timer = new Timer();
	
	private StartContext context;
	
	private Filter filter = null;
	
	public BackupRunnable(){
		timer.schedule(this, 1000 * 180, 1000 * 180);
	}
	
	public Task getConfig() {
		return config;
	}

	public void setConfig(Task config) {
		this.config = config;
	}

	public StartContext getContext() {
		return context;
	}

	public void setContext(StartContext context) {
		this.context = context;
	}

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}

	@Override
	public void run() {
		if (filter == null && context == null){
			return;
		}
		GridFS tracker_status = new GridFS(CrawlerMasterServer.getInstance().db,"tracker_stat");
		if (filter != null){
			String filename = config.name + "_" + config.collection + "_filter";
			byte[] filterData = filter.toBytes();
			tracker_status.remove(filename);
			GridFSInputFile file = tracker_status.createFile(filterData);
			file.setFilename(filename);
			file.save();
		}
		String filename = config.name + "_" + config.collection + "_context";
		tracker_status.remove(filename);
		byte[] contextData = context.toBytes();
		GridFSInputFile file = tracker_status.createFile(contextData);
		file.setFilename(filename);
		file.save();
	}


	@Override
	public void close(){
		timer.cancel();
	}

}
