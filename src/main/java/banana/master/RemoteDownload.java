package banana.master;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;


import banana.core.NodeStatus;
import banana.core.exception.DownloadException;
import banana.core.modle.Task;
import banana.core.protocol.DownloadProtocol;
import banana.core.request.Cookies;

public class RemoteDownload extends TimerTask {
	
	private static Logger logger = Logger.getLogger(RemoteDownload.class);
	
	private Timer timer = new Timer();
	
	private final String ip;
	
	private final int port;
	
	private DownloadProtocol downloadProtocol;
	
	private NodeStatus lastStatus;
	
	private long heartCheckPeriod = 1000 * 10;
	
	public RemoteDownload(String ip,int port,DownloadProtocol downloadProtocol) {
		this.ip = ip;
		this.port = port;
		this.downloadProtocol = downloadProtocol;
		timer.schedule(this, heartCheckPeriod, heartCheckPeriod);
	}
	

	public String getIp() {
		return ip;
	}

	public int getPort() {
		return port;
	}

	@Override
	public void run() {
		try {
			lastStatus = downloadProtocol.healthCheck();
			//logger.info("check health for "+ ip +" info " + lastStatus);
		} catch (Exception e) {
			logger.warn("check health failure for "+ ip,e);
			timer.cancel();
		}
	}
	
	public void setHeartCheckPeriod(long period){
		timer.cancel();
		timer = new Timer();
		timer.schedule(this, period, period);
	}

	public NodeStatus getLastStatus() {
		return lastStatus;
	}


	public boolean startDownloadTracker(String taskId,Task config,Cookies initCookie) throws DownloadException {
		return downloadProtocol.startDownloadTracker(taskId, config, initCookie);
	}

	public void resubmitTaskConfig(String taskId, int thread, Task config) throws DownloadException {
		downloadProtocol.resubmitTaskConfig(taskId, thread, config);
	}

	public boolean isWorking(String taskId) throws DownloadException {
		return downloadProtocol.isWorking(taskId);
	}

	public void stopDownloadTracker(String taskId) throws DownloadException {
		downloadProtocol.stopDownloadTracker(taskId);
	}

	public void stopDownloader(){
		try{
			downloadProtocol.stopDownloader();
		}catch(Exception e){
		}
		
	}

	public NodeStatus healthCheck() throws DownloadException {
		return downloadProtocol.healthCheck();
	}
	
	public void injectCookies(String taskId,Cookies cookies) throws DownloadException{
		downloadProtocol.injectCookies(taskId, cookies);
	}
}
