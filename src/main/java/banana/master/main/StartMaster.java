package banana.master.main;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import com.alibaba.fastjson.JSON;

import banana.core.protocol.CrawlerMasterProtocol;
import banana.core.protocol.Task;
import banana.master.impl.CrawlerMasterServer;

public class StartMaster {

	public static void main(String[] args) throws Exception {
		args = (args == null || args.length == 0)?new String[]{}:args;
		CommandLineParser parser = new DefaultParser();  
		Options options = new Options();
		options.addOption("h", "help", false, "print this usage information");  
		options.addOption("s", "submit", true, "submit task from a jsonfile");
		options.addOption("e", "extractor", true, "Set the extractor host");
		options.addOption("mdb", "mongodb", true, "Set the mongodb host and username/password");
		options.addOption("t", "test", true, "test task from a jsonfile");
		CommandLine commandLine = parser.parse(options, args); 
		HelpFormatter formatter = new HelpFormatter();
		if (commandLine.hasOption('h') ) {
		    formatter.printHelp("Master", options);
		    return;
		}
		if (!commandLine.hasOption('s') && (!commandLine.hasOption("mdb") || !commandLine.hasOption("e"))){
			System.out.println("Must have mongodb、extractor configuration");
			formatter.printHelp("Master", options);
			return;
		}
		String mongoAddress = commandLine.getOptionValue("mdb");
		String extractorAddress = commandLine.getOptionValue("e");
		//含有submit说明是提交任务，不含有则说明是启动master
		if (commandLine.hasOption('s')){
			String taskFilePath = commandLine.getOptionValue('s');
			Task task = initOneTask(taskFilePath);
			task.verify();
			boolean resubmit = false;
			Scanner scan = new Scanner(System.in);
			CrawlerMasterProtocol proxy = (CrawlerMasterProtocol) RPC.getProxy(CrawlerMasterProtocol.class,CrawlerMasterProtocol.versionID,new InetSocketAddress("localhost",8666),new Configuration());
			if (proxy.existTask(task.name).get()){
				System.out.print("Name for the task of "+ task.name +" already exists, do you want to update the configuration?\nConfirm the input y/yes:");
				String yes = scan.next();
				if (!yes.equalsIgnoreCase("Y") && !yes.equalsIgnoreCase("YES")){
					System.out.println("Task to submit cancel.");
					return;
				}
				resubmit = true;
			}
			if (proxy.taskdataExists(task.collection, task.name).get()){
				System.out.print("Do you need to remove before fetching result?\nConfirm the input y/yes:");
				String yes = scan.next();
				if (yes.equalsIgnoreCase("Y") || yes.equalsIgnoreCase("YES")){
					int n = proxy.removeBeforeResult(task.collection, task.name).get();
					System.out.println("Delete article " + n);
				}
			}
			if (!resubmit && proxy.statExists(task.collection, task.name).get()){
				System.out.print("Do you need to synchronize the previous state?\nConfirm the input y/yes:");
				String yes = scan.next();
				if (yes.equalsIgnoreCase("Y") || yes.equalsIgnoreCase("YES")){
					task.synchronizeStat = true;
				}
			}
			proxy.submitTask(task);
			System.out.println("Task to run");
		}else{
			CrawlerMasterServer crawlerMasterServer = new CrawlerMasterServer();
			crawlerMasterServer.setMasterPropertie("MONGO", mongoAddress);
			crawlerMasterServer.setMasterPropertie("EXTRACTOR", extractorAddress);
			crawlerMasterServer.init();
			if (crawlerMasterServer != null){
				Server server = new RPC.Builder(new Configuration()).setProtocol(CrawlerMasterProtocol.class)
		                .setInstance(crawlerMasterServer).setBindAddress("0.0.0.0").setPort(8666)
		                .setNumHandlers(100).build();
		        server.start();
				System.out.println("Master已经启动!!!可以陆续启动Downloader来扩展集群了");
			}
		}
	}
	
	public static Task initOneTask(String path) throws IOException{
		File file = new File(path);
		String json = FileUtils.readFileToString(file, "utf-8");
		Task task = JSON.parseObject(json, Task.class);
		task.data = json;
		return task;
	}
	
}
