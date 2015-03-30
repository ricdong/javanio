package net;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class MultiClient {
	
	private CountDownLatch latch = null;
	
	// keeps an address to server. 
	private InetSocketAddress address = new InetSocketAddress("localhost", 9000);
	
	private volatile boolean isRunning = true; 
	
	public MultiClient(int numThreads) {
		// we only need a latch 
		this.latch = new CountDownLatch(1); 
		Thread[] thds = new Thread[numThreads];
		for (int i = 0; i < thds.length; i++) {
			thds[i] = new Thread(new Worker(latch));
			thds[i].setDaemon(true);
			thds[i].start();
		}
		System.out.println(numThreads + " client are ready to call."); 
		
	}
	
	public void start() {
		this.latch.countDown();
	}
	
	public synchronized void join() throws InterruptedException {
		while (isRunning()) {
			wait();
		}
	}
	
	public boolean isRunning() {
		return this.isRunning;
	}
	
	class Worker implements Runnable {
		
		private CountDownLatch latch;
		
		NIOClient client = null; 
		
		public Worker(CountDownLatch latch) {
			this.latch = latch; 
			this.client = new NIOClient(); 
		}
		
		public void run() {
			
			while (isRunning()) {
				try {
					this.latch.await();
					byte[] data = NIOClient.random(128);
					client.call(data, address);
					
				} catch (InterruptedException | IOException e) {
				}
			}
		}
		
	}

	public static void main(String args[]) throws Exception {
		int numThreads = Runtime.getRuntime().availableProcessors(); 
		//int numThreads = 1;
		MultiClient mc = new MultiClient(numThreads);
		mc.start(); 
		mc.join();
	}
}
