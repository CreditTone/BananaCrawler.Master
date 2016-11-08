package banana.master.task;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;

import banana.master.impl.CrawlerMasterServer;


public class TaskTimer extends java.util.TimerTask{
	
	public banana.core.protocol.Task cfg;
	
	private Timer timer;
	
	public TaskTimer(banana.core.protocol.Task cfg){
		this.cfg = cfg;
		this.timer = new Timer();
	}
	
	public void start() throws ParseException{
		Date firstTime = null;
		if (cfg.timer.first_start.equals("now")){
			firstTime = new Date(System.currentTimeMillis()+1000);
		}else{
			String dateString = new SimpleDateFormat("yyyyMMdd").format(new Date());
			Date toDay = new SimpleDateFormat("yyyyMMdd HH:mm").parse(dateString + " " + cfg.timer.first_start);
			if (System.currentTimeMillis() > toDay.getTime()){
				firstTime = new Date(toDay.getTime() + (1000 * 3600 * 24));
			}else{
				firstTime = toDay;
			}
		}
		long period = 0;
		if (cfg.timer.period.contains("h")){
			int num = Integer.parseInt(cfg.timer.period.replace("h", ""));
			period = 1000 * 3600 * num;
		}else if (cfg.timer.period.contains("d")){
			int num = Integer.parseInt(cfg.timer.period.replace("d", ""));
			period = 1000 * 3600 * 24 * num;
		}else if (cfg.timer.period.contains("m")){
			int num = Integer.parseInt(cfg.timer.period.replace("m", ""));
			period = 1000 * 60 * num;
		}
		this.timer.schedule(this, firstTime, period);
	}
	
	public void stop(){
		this.timer.cancel();
	}
	
	@Override
	public void run() {
		try {
			TaskTracker tracker = new TaskTracker(cfg);
			CrawlerMasterServer.getInstance().tasks.put(tracker.getId(), tracker);
			tracker.start();
		} catch (Exception e) {
			e.printStackTrace();
			this.timer.cancel();
		}
	}

}
