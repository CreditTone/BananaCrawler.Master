package banana.master.task;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;

import banana.core.modle.TaskStatus.Stat;

public class TaskTimer extends java.util.TimerTask {

	public banana.core.modle.Task cfg;

	private Timer timer;
	
	public Stat stat = Stat.Timing;

	public TaskTracker tracker;
	
	public TaskTimer(banana.core.modle.Task cfg) {
		this.cfg = cfg;
		this.timer = new Timer();
	}

	public void start() throws Exception {
		Date firstTime = null;
		if (cfg.mode.timer.first_start.equals("now")) {
			firstTime = new Date(System.currentTimeMillis() + 10000);
		} else {
			String dateString = new SimpleDateFormat("yyyyMMdd").format(new Date());
			Date toDay = new SimpleDateFormat("yyyyMMdd HH:mm").parse(dateString + " " + cfg.mode.timer.first_start);
			if (System.currentTimeMillis() > toDay.getTime()) {
				firstTime = new Date(toDay.getTime() + (1000 * 3600 * 24));
			} else {
				firstTime = toDay;
			}
		}
		long period = 0;
		if (cfg.mode.timer.period.contains("h")) {
			int num = Integer.parseInt(cfg.mode.timer.period.replace("h", ""));
			period = 1000 * 3600 * num;
		} else if (cfg.mode.timer.period.contains("d")) {
			int num = Integer.parseInt(cfg.mode.timer.period.replace("d", ""));
			period = 1000 * 3600 * 24 * num;
		} else if (cfg.mode.timer.period.contains("m")) {
			int num = Integer.parseInt(cfg.mode.timer.period.replace("m", ""));
			period = 1000 * 60 * num;
		}
		this.timer.schedule(this, firstTime, period);
		System.out.println("timer schedule");
	}

	public void stop() {
		this.timer.cancel();
	}

	@Override
	public void run() {
		try {
			if (stat == Stat.Timing){
				tracker = new TaskTracker(cfg){
					@Override
					public void destoryTask() {
						super.destoryTask();
						stat = Stat.Timing;
						tracker = null;
					}
				};
				tracker.start();
				stat = Stat.Runing;
			}
		} catch (Exception e) {
			e.printStackTrace();
			this.timer.cancel();
		}
	}

}
