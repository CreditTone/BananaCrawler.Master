package com.banana.master.main;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.alibaba.fastjson.JSON;
import com.banana.master.impl.CrawlerMasterServer;

import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.DownloadProtocol;
import banana.core.protocol.Task;
import banana.core.util.SystemUtil;



public class StartMaster {

	public static void main2(String[] args) throws Exception {
		args = (args == null || args.length == 0)?new String[]{"-h"}:args;
		CommandLineParser parser = new BasicParser( );  
		Options options = new Options();  
		options.addOption("h", "help", false, "print this usage information");  
		options.addOption("r", "redis", true, "set the redis service. For example: 127.0.0.1:6379");
		options.addOption("s", "submit", false, "submit task from a file");
		CommandLine commandLine = parser.parse( options, args ); 
		HelpFormatter formatter = new HelpFormatter();
		if (commandLine.hasOption('h') ) {
		    formatter.printHelp("Master", options);
		    System.exit(0);
		}
		String redis = "localhost";
		int redisPort = 6379;
		if (commandLine.hasOption('r') ) {
			redis = commandLine.getOptionValue("r").split(":")[0];
			redisPort = Integer.parseInt(commandLine.getOptionValue("r").split(":")[1]);
		}
		//含有submit说明是提交任务，不含有则说明是启动master
		if (commandLine.hasOption('s')){
			String taskFilePath = commandLine.getOptionValue("s");
			Task task = initOneTask(taskFilePath);
			task.verify();
			CrawlerMasterProtocol proxy = (CrawlerMasterProtocol) RPC.getProxy(CrawlerMasterProtocol.class,CrawlerMasterProtocol.versionID,new InetSocketAddress("localhost",8686),new Configuration());
			proxy.startTask(task);
		}else{
			CrawlerMasterServer.init(redis, redisPort);
			CrawlerMasterProtocol crawlerMasterServer = CrawlerMasterServer.getInstance();
			if (crawlerMasterServer != null){
				String localIp = SystemUtil.getLocalIP();
				Server server = new RPC.Builder(new Configuration()).setProtocol(DownloadProtocol.class)
		                .setInstance(crawlerMasterServer).setBindAddress(localIp).setPort(8686)
		                .setNumHandlers(100).build();
		        server.start();
				System.out.println("Master已经启动!!!你可以陆续启动Downloader来扩展集群了");
			}
		}
	}
	
	public static List<Task> initTask(String path) throws IOException{
		List<Task> tasks = new ArrayList<Task>();
		File file = new File(path);
		String[] jsonFile = file.list(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".json");
			}
		});
		Task task = null;
		for (int i = 0; i < jsonFile.length; i++) {
			String json = FileUtils.readFileToString(new File(jsonFile[i]), "utf-8");
			task = JSON.parseObject(json, Task.class);
			tasks.add(task);
		}
		return tasks;
	}
	
	public static Task initOneTask(String path) throws IOException{
		File file = new File(path);
		String json = FileUtils.readFileToString(file, "utf-8");
		Task task = JSON.parseObject(json, Task.class);
		return task;
	}
	
}
