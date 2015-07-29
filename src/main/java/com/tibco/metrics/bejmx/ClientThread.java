package com.tibco.metrics.bejmx;

public class ClientThread implements Runnable {
	Client client;

	public ClientThread(Client client) {
		this.client = client;
	}
	
	public void run() {
		client.writeAllMetrics();
	}
}
