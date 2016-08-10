package com.banana.master;

import java.util.Timer;
import java.util.TimerTask;

import banana.core.NodeStatus;
import banana.core.exception.DownloadException;
import banana.core.protocol.DownloadProtocol;

public class RemoteDownload extends TimerTask{
	
	private Timer timer = new Timer();
	
	private String ip;
	
	private DownloadProtocol downloadProtocol;
	
	private NodeStatus lastStatus;
	
	private long heartCheckPeriod = 1000 * 10;
	
	public RemoteDownload() {
		timer.schedule(this, heartCheckPeriod);
	}

	@Override
	public void run() {
		try {
			lastStatus = downloadProtocol.healthCheck();
		} catch (DownloadException e) {
			e.printStackTrace();
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

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public DownloadProtocol getDownloadProtocol() {
		return downloadProtocol;
	}

	public void setDownloadProtocol(DownloadProtocol downloadProtocol) {
		this.downloadProtocol = downloadProtocol;
	}
	
}
