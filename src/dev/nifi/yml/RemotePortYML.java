package dev.nifi.yml;

import org.apache.nifi.api.toolkit.model.RemoteProcessGroupPortDTO;

import dev.nifi.yml.HelperYML.ReservedComponents;

public class RemotePortYML {
	public String name;
	public String type;

	public Integer maxConcurrentTasks;
	public Boolean useCompression;
	
	public Integer batchCount;
	public String batchSize;
	public String batchDuration;
	
	
	public RemotePortYML(RemoteProcessGroupPortDTO port, ReservedComponents type) {
		this.name = port.getName();
		this.type = type.name();
		
		if (port.getConcurrentlySchedulableTaskCount() != null && port.getConcurrentlySchedulableTaskCount() != 1) {
			this.maxConcurrentTasks = port.getConcurrentlySchedulableTaskCount();
		}
		if (!Boolean.FALSE.equals(port.getUseCompression())) {
			this.useCompression = port.getUseCompression();
		}
		
		// By default all batch settings are null, so any non-null value is good to serialize
		this.batchCount = port.getBatchSettings().getCount();
		this.batchSize = port.getBatchSettings().getSize();
		this.batchDuration = port.getBatchSettings().getDuration();
	}
}
