package com.banana.master.impl;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.apache.log4j.Logger;

import com.banana.common.download.IDownload;

public class RemoteDownload {

	private static Logger logger = Logger.getLogger(RemoteDownload.class);
	
	private String clientHost;
	
	private IDownload download;

	public RemoteDownload(String host) throws MalformedURLException, RemoteException, NotBoundException {
		this.clientHost = host;
		String rmiArress = "rmi://"+host+":1099/downloader";
		this.download = (IDownload) Naming.lookup(rmiArress);
	}

	public String getClientHost() {
		return clientHost;
	}
	
	
}
