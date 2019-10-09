package dev.nifi.commands;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ConnectionsApi;
import org.apache.nifi.api.toolkit.api.ControllerServicesApi;
import org.apache.nifi.api.toolkit.api.FlowApi;
import org.apache.nifi.api.toolkit.api.FlowfileQueuesApi;
import org.apache.nifi.api.toolkit.api.FunnelApi;
import org.apache.nifi.api.toolkit.api.InputPortsApi;
import org.apache.nifi.api.toolkit.api.LabelsApi;
import org.apache.nifi.api.toolkit.api.OutputPortsApi;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.api.ProcessorsApi;
import org.apache.nifi.api.toolkit.api.RemoteProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.ConnectionsEntity;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.ControllerServicesEntity;
import org.apache.nifi.api.toolkit.model.DropRequestEntity;
import org.apache.nifi.api.toolkit.model.FunnelEntity;
import org.apache.nifi.api.toolkit.model.FunnelsEntity;
import org.apache.nifi.api.toolkit.model.InputPortsEntity;
import org.apache.nifi.api.toolkit.model.LabelEntity;
import org.apache.nifi.api.toolkit.model.LabelsEntity;
import org.apache.nifi.api.toolkit.model.OutputPortsEntity;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessGroupsEntity;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.ProcessorsEntity;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupsEntity;

public class ClearCommand extends BaseCommand {

	private final String clientId = UUID.randomUUID().toString();
	

	private final FlowApi flowAPI = new FlowApi(getApiClient());
	private final FlowfileQueuesApi flowFileQueuesAPI = new FlowfileQueuesApi(getApiClient());
	private final ConnectionsApi connectionAPI = new ConnectionsApi(getApiClient());
	private final ProcessorsApi processorAPI = new ProcessorsApi(getApiClient());
	private final FunnelApi funnelAPI = new FunnelApi(getApiClient());
	private final InputPortsApi inputAPI = new InputPortsApi(getApiClient());
	private final OutputPortsApi outputAPI = new OutputPortsApi(getApiClient());
	private final LabelsApi labelAPI = new LabelsApi(getApiClient());
	private final ProcessGroupsApi processGroupAPI = new ProcessGroupsApi(getApiClient());
	private final RemoteProcessGroupsApi remoteGroupAPI = new RemoteProcessGroupsApi(getApiClient());
	private final ControllerServicesApi controllerServiceAPI = new ControllerServicesApi(getApiClient());
	
	private final SetStatusCommand disableCommand = new SetStatusCommand(false);
	
	public ClearCommand() {
		super();
	}

	@Override
	public void run() {
		try {
			// Disable everything first
			disableCommand.run();
			
			// Then delete everything out of the workspace
			clear("root");
		} catch (ApiException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void clear(String processGroupId) throws ApiException {
		
		// Collect all of the entities that are in this process group
		ProcessorsEntity root = processGroupAPI.getProcessors(processGroupId, false);
		ConnectionsEntity connections = processGroupAPI.getConnections(processGroupId);
		FunnelsEntity funnels = processGroupAPI.getFunnels(processGroupId);
		ProcessGroupsEntity pge = processGroupAPI.getProcessGroups(processGroupId);
		InputPortsEntity ipe = processGroupAPI.getInputPorts(processGroupId);
		OutputPortsEntity ope = processGroupAPI.getOutputPorts(processGroupId);
		LabelsEntity lbe = processGroupAPI.getLabels(processGroupId);
		RemoteProcessGroupsEntity rpge = processGroupAPI.getRemoteProcessGroups(processGroupId);
		ControllerServicesEntity cse = flowAPI.getControllerServicesFromGroup(processGroupId, false, false);
		

		for (ConnectionEntity conn : connections.getConnections()) {
			DropRequestEntity dropQueue = flowFileQueuesAPI.createDropRequest(conn.getId());
			while (dropQueue.getDropRequest().getFinished() != Boolean.TRUE) {
				try {
					Thread.sleep(TimeUnit.MILLISECONDS.convert(1, TimeUnit.SECONDS));
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				dropQueue = flowFileQueuesAPI.getDropRequest(conn.getId(), dropQueue.getDropRequest().getId());
			}
			connectionAPI.deleteConnection(conn.getId(), ""+conn.getRevision().getVersion(), clientId, true);
		}

		for (ProcessorEntity p : root.getProcessors()) {
			processorAPI.deleteProcessor(p.getId(), ""+p.getRevision().getVersion(), clientId, false);
		}
		
		for (FunnelEntity fn : funnels.getFunnels()) {
			funnelAPI.removeFunnel(fn.getId(), ""+fn.getRevision().getVersion(), clientId, false);
		}
		
		for (ProcessGroupEntity pg : pge.getProcessGroups()) {
			// Recurse into the Process group first and remove all its innards
			clear(pg.getId());
			processGroupAPI.removeProcessGroup(pg.getId(), ""+pg.getRevision().getVersion(), clientId, false);
		}
		
		for (PortEntity p : ipe.getInputPorts()) {
			inputAPI.removeInputPort(p.getId(), ""+p.getRevision().getVersion(), clientId, false);
		}
		
		for (PortEntity p : ope.getOutputPorts()) {
			outputAPI.removeOutputPort(p.getId(), ""+p.getRevision().getVersion(), clientId, false);
		}
		
		for (LabelEntity l : lbe.getLabels()) {
			labelAPI.removeLabel(l.getId(), ""+l.getRevision().getVersion(), clientId, false);
		}
		
		for (RemoteProcessGroupEntity rpg : rpge.getRemoteProcessGroups()) {
			remoteGroupAPI.removeRemoteProcessGroup(rpg.getId(), ""+rpg.getRevision().getVersion(), clientId, false);
		}
		
		for (ControllerServiceEntity cs : cse.getControllerServices()) {
			controllerServiceAPI.removeControllerService(cs.getId(), ""+cs.getRevision().getVersion(), clientId, false);
		}
	}
	
	// Tester main method
	public static void main(String[] args) {
		ClearCommand command = new ClearCommand();
		command.configureApiClients("localhost", "8080", false);
		command.run();
	}
}
