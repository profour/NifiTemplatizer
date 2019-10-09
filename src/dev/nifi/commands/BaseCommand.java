package dev.nifi.commands;

import org.apache.nifi.api.toolkit.ApiClient;

public abstract class BaseCommand implements Runnable {

	private static final ApiClient client = new ApiClient();
	
	protected static ApiClient getApiClient() {
		return client;
	}
	
	public void configureApiClients(String host, String port, boolean secure) {
		final String basePath = String.format("%s://%s:%s/nifi-api", secure ? "https" : "http", host, port);
		
		// Set API connection properties for all API endpoints
		ApiClient client = getApiClient();
		synchronized(client) {
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
