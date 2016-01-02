package com.banana.master.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.banana.common.download.IDownload;

public class LoadBalance {
	
	private static Logger logger = Logger.getLogger(LoadBalance.class);
	
	private long download_interval = 0;
	
	private LinkedBlockingQueue<IDownload> downloads = new LinkedBlockingQueue<IDownload>();
	
	private Map<IDownload,Long> downloadLastUseTime = new HashMap<IDownload,Long>();
	
	public LoadBalance(){
		downloads.addAll(CrawlerMasterServer.getInstance().getAllDownload());
	}
	
	public IDownload getDownload() {
		IDownload download = null;
		try{
			download = downloads.take();
			if (download != null){
				if (download_interval > 0){
					long lastUseTime = downloadLastUseTime.getOrDefault(download, 0L);
					while((System.currentTimeMillis() - lastUseTime) < download_interval){
						//等待
					}
				}
				downloadLastUseTime.put(download, System.currentTimeMillis());
				downloads.add(download);
			}
		}catch(InterruptedException e){
			logger.warn("", e);
		}
		return download;
	}
	
	public void remove(IDownload download){
		downloads.remove(download);
	}
	
}
