package banana.master.main;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;

import com.alibaba.fastjson.JSON;

import banana.core.modle.MasterConfig;
import banana.master.MasterServer;

public class Command {

	public static void main(String[] args) throws Exception {
		args = (args == null || args.length == 0) ? new String[] {} : args;
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("h", "help", false, "print this usage information");
		options.addOption("c", "config", true, "config file");
		CommandLine commandLine = parser.parse(options, args);
		HelpFormatter formatter = new HelpFormatter();
		if (commandLine.hasOption('h')) {
			formatter.printHelp("Master", options);
			return;
		}
		//启动master
		File configFile = new File("master_config.json");
		if (commandLine.hasOption("c")){
			configFile = new File(commandLine.getOptionValue("c"));
		}else if (!configFile.exists()){
			try{
				configFile = new File(Command.class.getClassLoader().getResource("").getPath() + "/master_config.json");
			}catch(Exception e){
				System.out.println("请指定配置文件位置");
				System.exit(0);
			}
		}
		MasterConfig config = JSON.parseObject(FileUtils.readFileToString(configFile),MasterConfig.class);
		MasterServer masterServer = new MasterServer(config);
		masterServer.start();
		System.out.println("Master已经启动!!!可以陆续启动Downloader来扩展集群了");
	}

}
