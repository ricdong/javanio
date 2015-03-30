package net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Random;

import javax.net.SocketFactory;

public class NIOClient implements VersionedProtocol {
	
	final SocketFactory socketFactory;
	
	private final HashMap<InetSocketAddress, Connection> connections; 
	
	public NIOClient() {
		this.socketFactory = SocketFactory.getDefault();
		this.connections = new HashMap<InetSocketAddress, Connection>(); 
	}
	
	private Connection getConnection(InetSocketAddress address)
			throws IOException {

		Connection connection = null;
		
		synchronized (connections) {
			connection = connections.get(address);
			if (connection == null) {
				connection = new Connection(address);
				connections.put(address, connection);
			}
		}

		connection.setupIOstreams();
		return connection;
	}
	
	public void call(byte[] data, InetSocketAddress address)
			throws IOException {
		Connection connection = getConnection(address); 
		connection.sendData(data);
	}
	
	class Connection extends Thread {
		
		private Socket socket = null; // connected socket  
		private DataInputStream in; 
		private DataOutputStream out; 
		
		private InetSocketAddress address;
		
		public Connection(InetSocketAddress remoteId) {
			this.address = remoteId;
			this.setName("R Client " + remoteId.toString());
			this.setDaemon(true);
		}
		
		public synchronized void setupIOstreams()throws IOException {
			if (socket != null) {
				return;
			}
			
			setupConnection();; 
			this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
			this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			
			writeHeader(); 
			
			// start the receiver thread after the socket connection has been set up 
			start(); 
		}
		
		private synchronized void setupConnection() throws IOException {
			while (true) {
				this.socket = socketFactory.createSocket();
				this.socket.setTcpNoDelay(true);
				this.socket.setKeepAlive(true);
				
				this.socket.connect(this.address, 20 * 1000); 
				this.socket.setSoTimeout(30 * 1000);
				System.out.println("getSendBufferSize" + this.socket.getSendBufferSize());
				return;
				// TODO handleConnectionFailure
			}
		}
		
		private void writeHeader() throws IOException {
			out.write(HEADER.array()); 
			out.writeByte(CURRENT_VERSION);
		}
		
		public void sendData(byte[] data)throws IOException {
			this.out.writeInt(data.length);
			this.out.write(data);
			synchronized (this.out) {
				this.out.flush();
			}
		}
		
		public void run() {

		}
		
	}
	
	static Random  r = new Random(); 
	
	public static byte[] random(int length) {
		byte[] arr = new byte[length];
		for (int i = 0; i < length; i++) 
			arr[i] = (byte)r.nextInt(127);
		return arr; 
	}
	
	

	public static void main(String[] args)throws IOException {
		
		InetSocketAddress address = new InetSocketAddress("localhost", 9000); 
		
		NIOClient client = new NIOClient();
		int total = 0; 
		String data = new String(random(1024 * 1024));
		for(;;){
			client.call(data.getBytes(), address);
			if(total++ % 500 == 0){
				System.out.println(total);
			}
			try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {
			}
		}
	}

}
