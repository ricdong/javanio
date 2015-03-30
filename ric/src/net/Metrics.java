package net;

import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {

	private AtomicInteger val = null;
	private final String name; 
	
	public Metrics(String name) {
		this.name = name; 
		this.val = new AtomicInteger();
		
		Thread thd = new Thread(new Watcher());
		thd.start();
	}
	
	public void touch() {
		this.val.getAndIncrement();
	}

	@Override
	public String toString() {
		return "Metrics [All touches =" + val + ", name=" + name + "]";
	}
	
	class Watcher implements Runnable {
		
		protected Watcher() {
			
		}
		
		public void run() {
			
			int before = val.get();
			while (true) {
				before = val.get();
				try {
					Thread.sleep(10 * 1000);
				} catch (InterruptedException e) {
				}
				
				System.out.println("current " + val.get() + ", " + (val.get() - before) / (10 * 1.00f) + " requests per seconds");
			}
		}
	}
	
}
