package banana.master;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.log4j.Logger;


import banana.core.NodeStatus;
import banana.core.protocol.DownloadProtocol;

public class RemoteDownload extends TimerTask{
	
	private static Logger logger = Logger.getLogger(RemoteDownload.class);
	
	private Timer timer = new Timer();
	
	private final String ip;
	
	private final int port;
	
	private DownloadProtocol downloadProtocol;
	
	private NodeStatus lastStatus;
	
	private long heartCheckPeriod = 1000 * 10;
	
	public RemoteDownload(String ip,int port) {
		this.ip = ip;
		this.port = port;
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


	public DownloadProtocol getDownloadProtocol() {
		return downloadProtocol;
	}

	public void setDownloadProtocol(DownloadProtocol downloadProtocol) {
		this.downloadProtocol = downloadProtocol;
	}
	
}
