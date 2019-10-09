package dev.nifi.commands;

import java.util.UUID;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ControllerServicesApi;
import org.apache.nifi.api.toolkit.api.FlowApi;
import org.apache.nifi.api.toolkit.api.InputPortsApi;
import org.apache.nifi.api.toolkit.api.OutputPortsApi;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.api.ProcessorsApi;
import org.apache.nifi.api.toolkit.api.RemoteProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.ControllerServiceRunStatusEntity;
import org.apache.nifi.api.toolkit.model.ControllerServicesEntity;
import org.apache.nifi.api.toolkit.model.InputPortsEntity;
import org.apache.nifi.api.toolkit.model.OutputPortsEntity;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.PortRunStatusEntity;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessGroupsEntity;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.ProcessorRunStatusEntity;
import org.apache.nifi.api.toolkit.model.ProcessorStatusDTO.RunStatusEnum;
import org.apache.nifi.api.toolkit.model.ProcessorsEntity;
import org.apache.nifi.api.toolkit.model.RemotePortRunStatusEntity;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupsEntity;
import org.apache.nifi.api.toolkit.model.RevisionDTO;

public class SetStatusCommand extends BaseCommand {
	
	private final String clientId = UUID.randomUUID().toString();

	private final FlowApi flowAPI = new FlowApi();
	private final ProcessorsApi processorAPI = new ProcessorsApi();
	private final ProcessGroupsApi processGroupAPI = new ProcessGroupsApi();
	private final RemoteProcessGroupsApi remoteProcessGroupAPI = new RemoteProcessGroupsApi();
	private final InputPortsApi inputPortAPI = new InputPortsApi();
	private final OutputPortsApi outputPortAPI = new OutputPortsApi();
	private final ControllerServicesApi controllerServiceAPI = new ControllerServicesApi();
	
	private final boolean enabled;
	
	public SetStatusCommand(boolean enabled) {
		super();
		
		this.enabled = enabled;
		
		// Register all of the ApiClients with the base command class
		addApiClients(flowAPI.getApiClient(), processorAPI.getApiClient(), processGroupAPI.getApiClient(), 
				inputPortAPI.getApiClient(), outputPortAPI.getApiClient(),
				remoteProcessGroupAPI.getApiClient(), controllerServiceAPI.getApiClient());
	}


	@Override
	public void run() {
		try {
			setStatus("root", enabled);
		} catch (ApiException e) {
			// TODO: implement proper logging
			e.printStackTrace();
		}
	}
	
	private void setStatus(String processGroupId, boolean enabled) throws ApiException {
		
		// Fetch all of the status-able entities on the workspace (inside the process group)
		// NOTE: the top level process group is aliased by "root", but its UUID also works
		ProcessorsEntity root = processGroupAPI.getProcessors(processGroupId, false);
		ProcessGroupsEntity pge = processGroupAPI.getProcessGroups(processGroupId);
		InputPortsEntity ipe = processGroupAPI.getInputPorts(processGroupId);
		OutputPortsEntity ope = processGroupAPI.getOutputPorts(processGroupId);
		RemoteProcessGroupsEntity rpge = processGroupAPI.getRemoteProcessGroups(processGroupId);
		ControllerServicesEntity cse = flowAPI.getControllerServicesFromGroup(processGroupId, false, false);
		
		// For all elements on the workspace, set their status (enabled or disabled)
		for (ProcessorEntity processor : root.getProcessors()) {
			setStatusProcessor(processor, enabled);
		}
		for (PortEntity input : ipe.getInputPorts()) {
			setStatusInputPort(input, enabled);
		}
		for (PortEntity output : ope.getOutputPorts()) {
			setStatusOutputPort(output, enabled);
		}
		for (ControllerServiceEntity controllerService : cse.getControllerServices()) {
			setStatusControllerService(controllerService, enabled);
		}
		for (ProcessGroupEntity processGroup : pge.getProcessGroups()) {
			// Recurse into the ProcessGroup, disabling everything inside of it
			setStatus(processGroup.getId(), enabled);
		}
		
		// Deal with RemoteProcessGroups last so that all of the ports/connections they deal with may be enabled first
		for (RemoteProcessGroupEntity remoteGroup : rpge.getRemoteProcessGroups()) {
			remoteGroup = remoteProcessGroupAPI.getRemoteProcessGroup(remoteGroup.getId());
			setStatusRemoteProcessGroup(remoteGroup, enabled);
		}
	}
	
	private void setStatusProcessor(ProcessorEntity processor, boolean enabled) throws ApiException {
		// In order to run a disabled processor, it must first go disabled -> stopped -> running
		if (processor.getStatus().getRunStatus() == RunStatusEnum.DISABLED && enabled) {
			ProcessorRunStatusEntity status = new ProcessorRunStatusEntity();
			status.setState(ProcessorRunStatusEntity.StateEnum.STOPPED);
			status.setRevision(createRevision(processor.getRevision().getVersion()));
			processorAPI.updateRunStatus(processor.getId(), status);
		}
		
		ProcessorRunStatusEntity status = new ProcessorRunStatusEntity();
		status.setState(enabled ? ProcessorRunStatusEntity.StateEnum.RUNNING : ProcessorRunStatusEntity.StateEnum.STOPPED);
		status.setRevision(createRevision(processor.getRevision().getVersion()));
		processorAPI.updateRunStatus(processor.getId(), status);
	}
	
	private void setStatusInputPort(PortEntity input, boolean enabled) throws ApiException {
		
		
		PortRunStatusEntity stat = new PortRunStatusEntity();
		stat.setState(enabled ? PortRunStatusEntity.StateEnum.RUNNING : PortRunStatusEntity.StateEnum.STOPPED);
		stat.setRevision(createRevision(input.getRevision().getVersion()));
		inputPortAPI.updateRunStatus(input.getId(), stat);
	}
	
	private void setStatusOutputPort(PortEntity output, boolean enabled) throws ApiException {
		PortRunStatusEntity stat = new PortRunStatusEntity();
		stat.setState(enabled ? PortRunStatusEntity.StateEnum.RUNNING : PortRunStatusEntity.StateEnum.STOPPED);
		stat.setRevision(createRevision(output.getRevision().getVersion()));
		outputPortAPI.updateRunStatus(output.getId(), stat);
	}
	
	private void setStatusRemoteProcessGroup(RemoteProcessGroupEntity remoteGroup, boolean enabled) throws ApiException {
		RemotePortRunStatusEntity groupStatus = new RemotePortRunStatusEntity();
		groupStatus.setState(enabled ? RemotePortRunStatusEntity.StateEnum.TRANSMITTING : RemotePortRunStatusEntity.StateEnum.STOPPED);
		groupStatus.setRevision(createRevision(remoteGroup.getRevision().getVersion()));
		remoteGroup = remoteProcessGroupAPI.updateRemoteProcessGroupRunStatus(remoteGroup.getId(), groupStatus);
	}
	
	private void setStatusControllerService(ControllerServiceEntity controllerService, boolean enabled) throws ApiException {
		ControllerServiceRunStatusEntity runStat = new ControllerServiceRunStatusEntity();
		runStat.setState(enabled ? ControllerServiceRunStatusEntity.StateEnum.ENABLED : ControllerServiceRunStatusEntity.StateEnum.DISABLED);
		runStat.setRevision(createRevision(controllerService.getRevision().getVersion()));
		controllerServiceAPI.updateRunStatus(controllerService.getId(), runStat);
	}
	
	// Helper method to create revision DTOs that all updateStatus requests require
	private RevisionDTO createRevision(long revision) {
		RevisionDTO dto = new RevisionDTO();
		dto.setClientId(clientId);
		dto.setVersion(revision);
		return dto;
	}
	
	// Tester main method
	public static void main(String[] args) {
		SetStatusCommand command = new SetStatusCommand(true);
		command.configureApiClients("localhost", "8080", false);
		command.run();
	}
}
