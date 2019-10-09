package dev.nifi.commands;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.FlowApi;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.api.RemoteProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.ConnectionEntity.DestinationTypeEnum;
import org.apache.nifi.api.toolkit.model.ConnectionsEntity;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.ControllerServicesEntity;
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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import dev.nifi.utils.DependencyBuilder;
import dev.nifi.yml.ControllerYML;
import dev.nifi.yml.ElementYML;
import dev.nifi.yml.TemplateYML;


public class ExportCommand extends BaseCommand {

	private final FlowApi flowAPI = new FlowApi(getApiClient());
	private final ProcessGroupsApi processGroupAPI = new ProcessGroupsApi(getApiClient());
	private final RemoteProcessGroupsApi remoteGroupAPI = new RemoteProcessGroupsApi(getApiClient());

	private final String outputDir;
	
	public ExportCommand(String outputDir) {
		super();
		
		if (outputDir == null) {
			// Default to current directory if nothing provided
			this.outputDir = ".";
		} else {
			this.outputDir = outputDir;
		}
	}

	@Override
	public void run() {

		try {
			// Start from the root and work our way down converting all process groups into templates
			List<TemplateYML> templates = new ArrayList<TemplateYML>();
			
			convertToTemplateYML("root", templates);
			
			// TODO: Look for structural duplicates in the templates and attempt to extract a common (parameterized) version
			
			export(templates);
		} catch (ApiException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private TemplateYML convertToTemplateYML(String processGroupId, List<TemplateYML> allTemplates) throws ApiException {
		
		// Pull all of the information we need to construct the template
		ProcessorsEntity root = processGroupAPI.getProcessors(processGroupId, false);
		ConnectionsEntity connections = processGroupAPI.getConnections(processGroupId);
		FunnelsEntity funnels = processGroupAPI.getFunnels(processGroupId);
		ProcessGroupsEntity pge = processGroupAPI.getProcessGroups(processGroupId);
		InputPortsEntity ipe = processGroupAPI.getInputPorts(processGroupId);
		OutputPortsEntity ope = processGroupAPI.getOutputPorts(processGroupId);
		LabelsEntity lbe = processGroupAPI.getLabels(processGroupId);
		RemoteProcessGroupsEntity rpge = processGroupAPI.getRemoteProcessGroups(processGroupId);
		ControllerServicesEntity cse = flowAPI.getControllerServicesFromGroup(processGroupId, false, false);
		
		
		Map<String, List<ConnectionEntity>> connectionLookup = new HashMap<>();
		for (ConnectionEntity ce : connections.getConnections()) {
			// When dealing with a destination that is a remote process group, 
			// the group id is the correct id, not the regular id
			String destination = ce.getDestinationType() == DestinationTypeEnum.REMOTE_INPUT_PORT ||
					ce.getDestinationType() == DestinationTypeEnum.INPUT_PORT 
					? ce.getDestinationGroupId() : ce.getDestinationId();
					
			if (!connectionLookup.containsKey(destination)) {
				connectionLookup.put(destination, new ArrayList<>());
			}
			
			connectionLookup.get(destination).add(ce);
		}
		
		
		// Start to convert into YAML
		TemplateYML rootPG = new TemplateYML();
		rootPG.name = processGroupId;
		allTemplates.add(rootPG);
		
		// Generate all of the dependencies needed
		DependencyBuilder depBuilder = new DependencyBuilder();
		rootPG.dependencies = depBuilder
				.addAllProcessorDependencies(root.getProcessors())
				.addAllControllerDependencies(cse.getControllerServices())
				.build();
		
		for (ControllerServiceEntity controller : cse.getControllerServices()) {
			ControllerYML c = new ControllerYML(controller, depBuilder.getCanonicalDependencyName(controller.getId()));
			rootPG.controllers.add(c);
		}
		
		for (PortEntity port : ipe.getInputPorts()) {
			ElementYML p = new ElementYML(port, connectionLookup.get(port.getId()));
			rootPG.components.add(p);
		}
		for (PortEntity port : ope.getOutputPorts()) {
			ElementYML p = new ElementYML(port, connectionLookup.get(port.getId()));
			rootPG.components.add(p);
		}
		
		for (ProcessorEntity pe : root.getProcessors()) {
			ElementYML p = new ElementYML(pe, depBuilder.getCanonicalDependencyName(pe.getId()), connectionLookup.get(pe.getId()));
			rootPG.components.add(p);
		}
		
		for (FunnelEntity f : funnels.getFunnels()) {
			ElementYML p = new ElementYML(f, connectionLookup.get(f.getId()));
			rootPG.components.add(p);
		}
		
		for (LabelEntity l : lbe.getLabels()) {
			ElementYML p = new ElementYML(l);
			rootPG.components.add(p);
		}
		
		for (ProcessGroupEntity pg : pge.getProcessGroups()) {
			TemplateYML template = convertToTemplateYML(pg.getId(), allTemplates);
			
			ElementYML p = new ElementYML(pg, template.name, connectionLookup.get(pg.getId()));
			rootPG.components.add(p);
		}
		
		for (RemoteProcessGroupEntity rpg : rpge.getRemoteProcessGroups()) {
			// We must refetch the remote process group information using the 
			// RemoteProcessGroupAPI to get all details on ports
			rpg = remoteGroupAPI.getRemoteProcessGroup(rpg.getId());
			ElementYML p = new ElementYML(rpg, connectionLookup.get(rpg.getId()));
			rootPG.components.add(p);
		}
		
		return rootPG;
	}
	
	private void export(List<TemplateYML> templates) throws IOException {
		YAMLFactory f = new YAMLFactory();
		f.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
		f.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
		ObjectMapper mapper = new ObjectMapper(f);
		
		mapper.setSerializationInclusion(Include.NON_EMPTY);
		
		for (TemplateYML template : templates) {
			String yaml = mapper.writer().writeValueAsString(template);
			
			// Formatting to make it easier to read the templates
			yaml = yaml.replaceAll("\n-", "\n\n-");
			yaml = yaml.replace("\ndependencies:", "\n\ndependencies:");
			yaml = yaml.replace("\ncontrollers:", "\n\ncontrollers:");
			yaml = yaml.replace("\ncontrollers:\n", "\ncontrollers:");
			yaml = yaml.replace("\ncomponents:", "\n\ncomponents:");
			yaml = yaml.replace("\ncomponents:\n", "\ncomponents:");
			
			try (FileWriter writer = new FileWriter(outputDir + File.separator + template.name + ".yaml")) {
				writer.write(yaml);
			}
		}
	}
	
	public static void main(String[] args) {
		ExportCommand command = new ExportCommand("./examples/simple/");
		command.configureApiClients("localhost", "8080", false);
		command.run();
	}
}
