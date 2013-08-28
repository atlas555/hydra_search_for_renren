package com.renren.redis.client;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

public class SocketThread extends Thread {	
	private static Logger logger = Logger.getLogger(SocketThread.class);
	private ServerSocket server = null;
	
	public SocketThread(int port) throws Exception {
		server = new ServerSocket(port);
	}
	
	public void run() {
		Socket socket = null;
		while(true) {
			try {
				logger.info("SocketThread run,accept~");	
				socket = server.accept();								
				socket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				logger.error("run error~"+e.toString());
				try {
					server.close();
				} catch(IOException ex) {
					logger.error("close ServerSocket:"+ex.toString());
				}				
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws UnknownHostException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		SocketThread st= null;
		try {
			st = new SocketThread(10020);
			st.start();
		} catch(Exception e) {
			System.out.println(e.toString());
		}		
		
		
		Socket sk = new Socket("127.0.0.1",10020);
		System.out.println("return st:"+sk);
		System.out.println("connect:"+sk.isConnected());
	
//		int i =0;
//		while(i<5) {
//			
//	    Socket sk = new Socket("127.0.0.1",10020);
//		System.out.println("return st:"+sk);
//		System.out.println("connect:"+sk.isConnected());
//			
//		BufferedReader in;
//		PrintWriter out; 
//		in = new BufferedReader(new InputStreamReader(sk.getInputStream()));
//		out = new PrintWriter(sk.getOutputStream(),true);
//		BufferedReader line = new BufferedReader(new InputStreamReader(System.in));
//		System.out.println(line.readLine());
//		in.close();
//		out.close();
//		sk.close();
//		i++;
//		}
	}
}
