package net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class NIOServer implements Runnable, VersionedProtocol {

	Thread thread = null;

	// the selector that we use for the server
	private Selector selector = null;

	ServerSocketChannel serverChannel = null;
	
	private Reader[] readers = null; 
	private ExecutorService readPool = null; 
	private Metrics metrics = new Metrics("NIOServer");
	
	protected BlockingQueue<Call> callQueue; // queued calls
	
	public NIOServer() throws Exception {
		int port = 9000;
		int readThreads = 1; 
		this.callQueue = new LinkedBlockingQueue<Call>(128);
		this.serverChannel = ServerSocketChannel.open();

		this.serverChannel
				.bind(new InetSocketAddress(InetAddress.getByName("0.0.0.0"),
						port), 1024);
		this.serverChannel.configureBlocking(false);

		readers = new Reader[readThreads]; 
		readPool = Executors.newFixedThreadPool(readThreads,
		        new ThreadFactoryBuilder().setNameFormat(
		          "IPC Reader %d on port " + port).setDaemon(true).build());
		
		for (int i = 0; i < readers.length; i++) {
			Reader reader = new Reader(); 
			readers[i] = reader;
			readPool.execute(reader);
		}
		
		// create the selector
		selector = Selector.open();
		this.serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		System.out.println("IPC Server listener on " + port);
	}
	
	int currentReader = 0;
	Reader getReader() {
		currentReader = (currentReader + 1) % readers.length;
		return readers[currentReader];
	}

	public void run() {
		System.out.println("starting");

		while (true) { // isrunning
			SelectionKey key = null;

			try {
				selector.select();
				Iterator<SelectionKey> iter = selector.selectedKeys()
						.iterator();

				while (iter.hasNext()) {
					key = iter.next();
					iter.remove();
					if (key.isValid()) {
						if (key.isAcceptable()) {
							System.out.println("accept a new connectioin.");
							doAccept(key);
						}
					}
				}

			} catch (IOException e) {

				e.printStackTrace();
			}
		}
	}

	void doAccept(SelectionKey key) throws IOException {
		ServerSocketChannel server = (ServerSocketChannel) key.channel();

		SocketChannel channel = null;
		while ((channel = server.accept()) != null) {
			channel.configureBlocking(false);
			channel.socket().setTcpNoDelay(true);
			channel.socket().setKeepAlive(true);
			System.out.println("getReceiveBufferSize " + channel.socket().getReceiveBufferSize());
			
			// a new connection 
			Reader reader = getReader(); 
			reader.startAdd();
			SelectionKey readkey = reader.registerChannel(channel);
			Connection con = new Connection(channel); 
			readkey.attach(con);
			
			reader.finishAd(); // should be in a finally block. 
		}
	}

	private class Reader implements Runnable {

		private final Selector readSelector;
		
		private volatile boolean isAdding = false; 

		public Reader() throws IOException {
			this.readSelector = Selector.open();
		}
		
		public void startAdd(){
			this.isAdding = true; 
			this.readSelector.wakeup();
		}
		
		public synchronized void finishAd(){
			this.isAdding = false;
			this.notify();
		}
		
		int count = 0; 

		public synchronized void run() {
			System.out.println("start Reader: ");
			while (true) {
				SelectionKey key = null; 
				
				try {
					this.readSelector.select();
					// System.out.println("wakeup..");
					
					while(isAdding) {
						wait(1000);
					}
					
					Iterator<SelectionKey> iter = this.readSelector.selectedKeys().iterator();
					
					System.out.println(iter.hasNext()); 
					
					int total = 1; 
					int count = 0; 
					while(iter.hasNext()){
						key = iter.next();
						iter.remove();
						if (key.isValid() && key.isReadable()) {
							Connection con = (Connection)key.attachment();
							if(++count % 5000 == 0){
								count = 0; 
							}
							count = con.readAndProcess(); // TODO would be throw exceptions. 
							
							if (count < 0) {
								System.err.println("disconnecting client " + con.getHostAddress());
								closeConnection(con); 
								
							}
							
							// System.out.println("read event..." + total++);
						}
						
						key = null; 
					}
					
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		// a new connection, register op_read event for this channel on current selector 
		public synchronized SelectionKey registerChannel(SocketChannel channel) 
				throws IOException {
			return channel.register(readSelector, SelectionKey.OP_READ);
		}
	}
	
	class Connection {
		
		private SocketChannel channel = null; 
		private ByteBuffer dataLengthBuffer;
		
		private boolean versionRead = false; 
		private ByteBuffer data;
		private int dataLength; 
		private Socket socket; 
		
		protected String hostAddress; 
		protected final LinkedList<Call> responseQueue; 
		
		public Connection(SocketChannel channel) {
			this.channel = channel; 
			this.dataLengthBuffer = ByteBuffer.allocate(4);
			this.socket = this.channel.socket(); 
			this.hostAddress = this.channel.socket().getInetAddress().getHostAddress();
			this.responseQueue = new LinkedList<Call>(); 
		}
		
		public String getHostAddress() {
			return this.hostAddress;
		}
		
		public int readAndProcess() throws IOException {
			while (true) {
				int count; 
				// HEADER
				if (this.dataLengthBuffer.remaining() > 0) {
					count = channelRead(channel, dataLengthBuffer);
					if(count < 0){
						System.err.println("nothing to be read");
						// TODO kill the connection
						return count; 
					}
				}
				
				if (!this.versionRead) {
					ByteBuffer versionBuf = ByteBuffer.allocate(1);
					count = channelRead(channel, versionBuf);
					if (count <= 0) {
						return count;
					}
					
					int version = versionBuf.get(0);
					this.dataLengthBuffer.flip();
					if (!HEADER.equals(dataLengthBuffer)
							|| version != CURRENT_VERSION) {
						System.err.println("Incorrect header or version mismatch");
						
						return -1; 
					}
					this.dataLengthBuffer.clear();
					versionRead = true; 
					continue; 
					// end for HEADER and VERSION reading. 
				}
				
				if (data == null) {
					dataLengthBuffer.flip(); 
					dataLength = dataLengthBuffer.getInt(); 
					
					// TODO is a PING call ID ?
//					if(dataLength = 1){
//						
//					}
					data = ByteBuffer.allocate(dataLength); 
				}
				
				count = channelRead(channel, data);
				
				if (data.remaining() == 0) {
					dataLengthBuffer.clear(); 
					data.flip(); 
					processData(data.array()); 
					data = null; 
					return count; 
				}
				
				return count; 
			}
		}
		
		int total = 0; 
		protected void processData(byte[] buf) throws IOException {
			System.out.println("package id: " + total++ + ", " + new String(buf));
			//System.out.println("data length " + buf.length);
			// touch the line 
			metrics.touch();
		}
		
		int channelRead(ReadableByteChannel channel, ByteBuffer buffer)
				throws IOException {
			// TODO shuold not be more than 64KB
			int count = channel.read(buffer);
			return count; 
		}
		
		protected synchronized void close() {
			data = null; 
			this.dataLengthBuffer = null;
			if (!channel.isOpen()) {
				// is this normal ?
				return; 
			}
			try {
				socket.shutdownOutput();
			} catch (IOException ignored) {
			}
			
			if (this.channel.isOpen()) {
				try {
					channel.close();
				} catch (IOException ignored) {
				} 
			}
			try {
				socket.close();
			} catch (IOException ignored) {
			} 
			
		}
		
	}
	
	/** A call queued for handling. a Call Context */
	class Call {
		
		protected Connection connection; 
		protected ByteBuffer data; 
		protected Responder responder; 
		
		
		public Call(Connection connection, Responder responder, ByteBuffer data) {
			this.connection = connection; 
			this.responder = responder; 
			this.data = data; 
		}
		
		public synchronized void sendResponse() throws IOException {
			this.responder.doRespond(this);
		}
		
	}
	
	/**
	 * Handles queued calls.  
	 * @author ricdong
	 */
	private class Handler extends Thread {
		
		private final BlockingQueue<Call> queue; 
		
		public Handler(BlockingQueue<Call> queue) {
			this.queue = queue;
		}
		
		public void run() {
			while (true) {
				System.out.println("waiting for a call");
				try {
					Call call = queue.take();  // pop the queue; may be blocked
					
					// TODO make a call ? 
					
					call.sendResponse();
					
				} catch (Throwable t) {
					// TODO 
				}
			}
		}
	}
	
	/** class responsible for sending response to clients.  */
	protected class Responder extends Thread {
		
		private final Selector writeSelector;
		private int pending; // connections waiting to register 
		
		Responder() throws IOException {
			this.writeSelector = Selector.open();  // create a selector
			this.setDaemon(true);
			this.pending = 0; 
		}
		
		public void run() {
			while (true) {
				// TODO 
			}
		}
		
		// call waiting to be enqueued
		private synchronized void incPending(){
			pending++; 
		}
		
		// call done enqueued
		private synchronized void decPending() {
			pending--;
			notify();
		}
		
		private synchronized void waitPending() throws InterruptedException {
			while (pending > 0) {
				wait(); 
			}
		}
		
		// returns true if there are no more pending data for this channel
		private boolean processResponse(final LinkedList<Call> queue)
				throws IOException {
			boolean done = false; // there ismore data for this channel
			int num; 
			
			Call call = null; 
			try {
				synchronized (queue) {
					
					// if there no items for this channel, we done
					num = queue.size(); 
					if (num == 0) {
						return true;  // no more data for this channel 
					}
					
					// extract the first call 
					call = queue.peek(); 
					SocketChannel channel = call.connection.channel;
					// send as much data as we can 
					// in the non-blocking fashion 
					int numBytes = channelWrite(channel, call.data);
					if (numBytes < 0) {
						// error flag, closes connection, and clears response queue. 
						return true; 
					}
					
					if (!call.data.hasRemaining()) {
						queue.poll(); // all data write out 
						if (num == 1) {
							// this is a last call 
							done = true; 
						} else {
							done = false;
						}
					} else {
						System.err.println("wrote partial " + numBytes + " bytes.");
					}
				}
			} finally {
				// check connection 
			}
			return done; 
		}
		
		private int channelWrite(WritableByteChannel channel, ByteBuffer buffer)
				throws IOException {
			return channel.write(buffer);
		}
		
		
		//
		// Enqueue a response form the application 
		//
		
		/**
		 * The method will be invoked by multi-threads
		 * Ric dong  
		 */
		void doRespond(Call call) throws IOException {
			boolean doRegister = false; 
			
			synchronized (call.connection.responseQueue) {
				// is the connection has closed ? please make a check 
				
				call.connection.responseQueue.addLast(call);
				
				// RicDong: if the send buffer is empty, we can send data immediately without channel registered
				if (call.connection.responseQueue.size() == 1) {
					doRegister = !processResponse(call.connection.responseQueue); 
				}
				
			}
			
			if(doRegister) {
				enqueueInSelector(call); 
			}
			
			// is connection closed ?
		}
		
		// Enqueue for background thread to send responses out later. 
		private boolean enqueueInSelector(Call call) throws IOException {
			boolean done = false; 
			incPending(); 
			
			try {
				// wake up the thread blocked on select,
				SocketChannel channel = call.connection.channel;
				writeSelector.wakeup(); 
				channel.register(writeSelector, SelectionKey.OP_WRITE, call);
			} finally {
				decPending(); 
			}
			// TODO we should catch the closedChannelException. 
			return true; 
		}
		
	}
	
	protected void closeConnection(Connection connection) {
		// TODO release any resource 
		connection.close(); 
	}

	public synchronized void join() throws Exception {
		this.wait();
	}

	public static void startServer() throws Exception {
		NIOServer server = new NIOServer();
		server.thread = new Thread(server);
		server.thread.start();
		server.join();
	}
	

	public static void main(String[] args) throws Exception {
		startServer();
	}

}
