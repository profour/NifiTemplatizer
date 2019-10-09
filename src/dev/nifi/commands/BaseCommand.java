package dev.nifi.commands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.nifi.api.toolkit.ApiClient;

public abstract class BaseCommand implements Runnable {
	
	private List<ApiClient> clients = new ArrayList<ApiClient>();
	
	protected void addApiClients(ApiClient ... clients) {
		for (ApiClient client : clients) {
			this.clients.add(client);
		}
	}
	
	protected void addApiClients(List<ApiClient> clients) {
		this.clients.addAll(clients);
	}
	
	protected List<ApiClient> getApiClients() {
		return Collections.unmodifiableList(clients);
	}
	
	public void configureApiClients(String host, String port, boolean secure) {
		final String basePath = String.format("%s://%s:%s/nifi-api", secure ? "https" : "http", host, port);
		
		// Set API connection properties for all API endpoints
		for (ApiClient client : clients) {
			client.setBasePath(basePath);
			// TODO: Enable flag for debugging
//			client.setDebugging(debugging)
			
			// TODO: Set Authentication stuff
//			client.setUsername(arg0);
//			client.setPassword();
//			client.setApiKey(arg0);
			
			// TODO: Setup SSL stuff
//			client.setSslCaCert(sslCaCert)
//			client.setVerifyingSsl(verifyingSsl)
		}
	}
}
