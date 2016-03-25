package com.banana.master.impl;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.banana.common.download.IDownload;

public class RemoteDownload {

	private static Logger logger = Logger.getLogger(RemoteDownload.class);
	
	private Map<String,IDownload> downloads = new HashMap<String,IDownload>();
	
	private Map<String,Integer> downloadThread = new HashMap<String,Integer>();

	public RemoteDownload(List<String> downloadHost) throws MalformedURLException, RemoteException, NotBoundException {
		setDownloads(downloadHost);
	}
	
	public RemoteDownload(){}
	
	public void setDownloads(List<String> downloadHost)throws MalformedURLException, RemoteException, NotBoundException{
		downloads.clear();
		for (String clientHost : downloadHost) {
			String rmiAddress = "rmi://"+clientHost+":1099/downloader";
			IDownload download = (IDownload) Naming.lookup(rmiAddress);
			downloads.put(clientHost, download);
		}
	}
	
	public void setDownloadThread(String downloadHost,int thread){
		downloadThread.put(downloadHost, thread);
	}
	
	public void addDownloadThread(String downloadHost,int thread){
		Integer threadNum = downloadThread.get(downloadHost);
		if (threadNum == null){
			threadNum = 0;
		}
		threadNum += thread;
		downloadThread.put(downloadHost, thread);
	}
	
	public void startCrawl(String taskName) throws RemoteException{
		for (String downloadHost : downloads.keySet()) {
			IDownload d = downloads.get(downloadHost);
			d.newDownloadTracker(taskName, downloadThread.get(downloadHost));
			d.startDownloadTracker(taskName);
		}
	}
	
	
}
