package dev.nifi.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ConnectionsApi;
import org.apache.nifi.api.toolkit.api.ControllerApi;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.BundleDTO;
import org.apache.nifi.api.toolkit.model.ConnectableDTO;
import org.apache.nifi.api.toolkit.model.ConnectableDTO.TypeEnum;
import org.apache.nifi.api.toolkit.model.ConnectionDTO;
import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.ControllerEntity;
import org.apache.nifi.api.toolkit.model.ControllerServiceDTO;
import org.apache.nifi.api.toolkit.model.ControllerServiceEntity;
import org.apache.nifi.api.toolkit.model.FunnelDTO;
import org.apache.nifi.api.toolkit.model.FunnelEntity;
import org.apache.nifi.api.toolkit.model.LabelDTO;
import org.apache.nifi.api.toolkit.model.LabelEntity;
import org.apache.nifi.api.toolkit.model.PortDTO;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessorConfigDTO;
import org.apache.nifi.api.toolkit.model.ProcessorDTO;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupContentsDTO;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupDTO;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupPortDTO;
import org.apache.nifi.api.toolkit.model.RevisionDTO;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import dev.nifi.utils.ObjectTracker;
import dev.nifi.utils.Pair;
import dev.nifi.yml.ControllerYML;
import dev.nifi.yml.ElementYML;
import dev.nifi.yml.HelperYML;
import dev.nifi.yml.InputConnectionYML;
import dev.nifi.yml.HelperYML.ReservedComponents;
import dev.nifi.yml.TemplateYML;

public class ImportCommand extends BaseCommand {

	private final ProcessGroupsApi processGroupAPI = new ProcessGroupsApi(getApiClient());
	private final ControllerApi controllerAPI = new ControllerApi(getApiClient());
	private final ConnectionsApi connectionAPI = new ConnectionsApi(getApiClient());
	
	private final String importDir;
	
	private ObjectTracker tracker = new ObjectTracker();
	
	private String rootUUID = null;
	
	public ImportCommand(final String importDir) {
		super();
		
		if (importDir == null) {
			this.importDir = ".";
		} else {
			this.importDir = importDir;
		}
	}

	@Override
	public void run() {
		try {
			// Load templates from disk
			List<TemplateYML> templates = loadTemplates(importDir);

			// TODO: Consider using these to determine if all dependencies are available
			// ControllerServiceTypesEntity cs = fapi.getControllerServiceTypes(null, null, null, null, null, null, null);
			// ProcessorTypesEntity pt = fapi.getProcessorTypes(null, null, null);
			
			// Build a lookup database for template names
			Map<String, TemplateYML> templateDB = new HashMap<>();
			for (TemplateYML template : templates) {
				// References are by filename so add the extension ".yaml" for simple lookups
				templateDB.put(template.name + HelperYML.YAML_EXT, template); 
			}
			
			// Start with the root template
			importTemplate("root", templateDB.get("root.yaml"), templateDB);
		} catch (JsonParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ApiException e) {
			// TODO Auto-generated catch block
			System.out.println(e.getResponseBody());
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void importTemplate(String processGroupId, TemplateYML template, Map<String, TemplateYML> templateDB) throws ApiException {
		// Get the real UUID name for "root"
		if (processGroupId.equals("root")) {
			ProcessGroupEntity rootPG = processGroupAPI.getProcessGroup(processGroupId);
			processGroupId = rootPG.getId();
			rootUUID = processGroupId;
		}
		
		// Ensure all process groups are populated first
		createProcessGroups(processGroupId, template, templateDB);
		
		// Create all controller services that may be needed to service processor elements
		createControllerServices(processGroupId, template, templateDB);
		
		// Create all of the canvas elements (recursively)
		createElements(processGroupId, template, templateDB);
		
		// Linkage must run after create elements to ensure all src/dst pairs can be satisfied
		createLinkage(processGroupId, template);
		
	}
	
	private void createProcessGroups(String processGroupId, TemplateYML template, Map<String, TemplateYML> templateDB) throws ApiException {
		
		for (ElementYML ele : template.components) {
			// Only deal with Process Groups
			if (HelperYML.isProcessGroup(ele.type)) {
				ReservedComponents type = HelperYML.ReservedComponents.valueOf(ele.getType());
				
				PositionDTO position = HelperYML.createPosition(ele.position);
				
				switch (type) {
				case PROCESS_GROUP:
				{
					makeProcessGroup(processGroupId, position, ele, templateDB);
					break;
				}
				case REMOTE_PROCESS_GROUP:
				{
					makeRemoteProcessGroup(processGroupId, position, ele);
					break;
				}
				default:
					// Ignore all non process group types
				}
			}
		}
		
	}
	
	private void createControllerServices(String processGroupId, TemplateYML template, Map<String, TemplateYML> templateDB) throws ApiException {

		// Canonical type name -> (Type, Bundle)
		Map<String, Pair<String, BundleDTO>> depLookup = createDependencyLookup(template.dependencies);
		
		for (ControllerYML controller : template.controllers) {
			
			ControllerServiceEntity cont = new ControllerServiceEntity();
			ControllerServiceDTO dto = new ControllerServiceDTO();
			cont.setComponent(dto);
			cont.setRevision(getRevision());
			dto.setName(controller.name);
			
			// Lookup the typing for this based on the canonical type
			Pair<String, BundleDTO> dep = depLookup.get(controller.getType());
			dto.setBundle(dep.t2);
			dto.setType(dep.t1);
			
			if (controller.properties != null) {
				Map<String, String> props = new HashMap<String, String>();
				for (String key : controller.properties.keySet()) {
					props.put(key, controller.properties.get(key).toString());
				}
				dto.setProperties(props);
			}
			
			ControllerServiceEntity response = processGroupAPI.createControllerService(processGroupId, cont);
			
			tracker.track(processGroupId, controller.id, controller.getType(), response.getId());
			tracker.track(controller.id, response.getId());
		}
	}
	
	private void createElements(String processGroupId, TemplateYML template, Map<String, TemplateYML> templateDB) throws ApiException {
		
		// Canonical type name -> (Type, Bundle)
		Map<String, Pair<String, BundleDTO>> depLookup = createDependencyLookup(template.dependencies);
		
		for (ElementYML ele : template.components) {
			System.out.println(ele.getType());
			Pair<String, BundleDTO> dep = depLookup.get(ele.getType());
			System.out.println(dep);
			PositionDTO position = HelperYML.createPosition(ele.position);
			
			if (dep != null) {
				makeProcessor(processGroupId, ele, dep, position);
			} else {
				ReservedComponents type = HelperYML.ReservedComponents.valueOf(ele.getType());
				switch (type) {
				case FUNNEL:
				{
					makeFunnel(processGroupId, position, ele);
					break;
				}
				case INPUT_PORT:
				{
					makeInputPort(processGroupId, position, ele);
					break;
				}
				case LABEL:
				{
					makeLabel(processGroupId, position, ele);
					break;
				}
				case OUTPUT_PORT:
				{
					makeOutputPort(processGroupId, position, ele);
					break;
				}
				default:
					// Ignore the process group stuff that were already handled
				}
			}
		}
	}
	
	private void createLinkage(String processGroupId, TemplateYML template) throws ApiException {
		
		Map<String, ElementYML> lookup = new HashMap<String, ElementYML>();
		for (ElementYML element : template.components) {
			lookup.put(element.id, element);
		}
		
		// For all components, connect their inputs
		for (ElementYML destinationElement : template.components) {
			if (destinationElement.inputs == null) {
				continue;
			}

			
			for (InputConnectionYML input : destinationElement.inputs) {
				ElementYML sourceElement = lookup.get(input.source);
				

				System.out.println("DST: " + destinationElement.name + " : " + destinationElement.id + " " + destinationElement.type);

				System.out.println("SRC: " + sourceElement.name + " : " + sourceElement.id + " " + sourceElement.type);
				
				ConnectionEntity conn = new ConnectionEntity();
				conn.setRevision(getRevision());
				
				ConnectionDTO dto = new ConnectionDTO();
				
				// Set the source of the connection
				ConnectableDTO src = new ConnectableDTO();
				if (HelperYML.isPort(sourceElement.type)) {
					String groupId = tracker.getIdForObject(processGroupId, sourceElement.id, HelperYML.ReservedComponents.PROCESS_GROUP.name());
					groupId = (groupId == null) ? processGroupId : groupId;
					String portId = tracker.getIdForObject(groupId, sourceElement.name, sourceElement.type);
					src.setGroupId(groupId);
					src.setId(portId);
					src.setType(TypeEnum.valueOf(sourceElement.type));
				} else if (HelperYML.ReservedComponents.PROCESS_GROUP.isType(sourceElement.type)) {
					String groupId = tracker.getIdForObject(processGroupId, sourceElement.id, sourceElement.type);
					String portId = tracker.getIdForObject(groupId, input.from.get(0), HelperYML.ReservedComponents.OUTPUT_PORT.name());
					src.setGroupId(groupId);
					src.setId(portId);
					src.setName(input.from.get(0));
					src.setType(TypeEnum.OUTPUT_PORT);
				} else if (HelperYML.ReservedComponents.REMOTE_PROCESS_GROUP.isType(sourceElement.type)) {
					String groupId = tracker.getIdForObject(processGroupId, sourceElement.id, sourceElement.type);
					String portId = tracker.getIdForObject(groupId, input.from.get(0), HelperYML.ReservedComponents.OUTPUT_PORT.name());
					src.setGroupId(groupId);
					src.setId(groupId);
					src.setName(input.from.get(0));
					src.setType(TypeEnum.REMOTE_OUTPUT_PORT);
					continue;
				} else {
					src.setGroupId(processGroupId);

					String id = tracker.getIdForObject(processGroupId, sourceElement.id, sourceElement.type);
					src.setId(id);
					try {
						src.setType(TypeEnum.valueOf(sourceElement.type));
					} catch (Exception e) {
						if (HelperYML.isProcessGroup(sourceElement.type)) {
							src.setType(TypeEnum.OUTPUT_PORT);
						} else {
							src.setType(TypeEnum.PROCESSOR);
						}
					}
				}
				dto.setSource(src);
				
				// Set the destination of the connection
				ConnectableDTO dst = new ConnectableDTO();
				if (HelperYML.isPort(destinationElement.type)) {
					String groupId = tracker.getIdForObject(processGroupId, destinationElement.id, HelperYML.ReservedComponents.PROCESS_GROUP.name());
					groupId = (groupId == null ) ? processGroupId : groupId;
					String portId = tracker.getIdForObject(groupId, destinationElement.name, destinationElement.type);
					dst.setGroupId(groupId);
					dst.setId(portId);
					dst.setType(TypeEnum.valueOf(destinationElement.type));
				} else if (HelperYML.ReservedComponents.PROCESS_GROUP.isType(destinationElement.type)) {
					String groupId = tracker.getIdForObject(processGroupId, destinationElement.id, destinationElement.type);
					String portId = tracker.getIdForObject(groupId, input.to, HelperYML.ReservedComponents.INPUT_PORT.name());
					dst.setGroupId(groupId);
					dst.setId(portId);
					dst.setName(input.to);
					dst.setType(TypeEnum.INPUT_PORT);
				} else if (HelperYML.ReservedComponents.REMOTE_PROCESS_GROUP.isType(destinationElement.type)) {
					String groupId = tracker.getIdForObject(processGroupId, destinationElement.id, destinationElement.type);
					String portId = tracker.getIdForObject(groupId, destinationElement.id, HelperYML.ReservedComponents.INPUT_PORT.name());
					dst.setGroupId(groupId);
					dst.setId(groupId);
					dst.setName(input.from.get(0));
					dst.setType(TypeEnum.REMOTE_INPUT_PORT);
					continue;
				} else {
					dst.setGroupId(processGroupId);
					
					String id = tracker.getIdForObject(processGroupId, destinationElement.id, destinationElement.type);
					dst.setId(id);
					
					try {
						dst.setType(TypeEnum.valueOf(destinationElement.type));
					} catch (Exception e) {
						if (HelperYML.isProcessGroup(sourceElement.type)) {
							dst.setType(TypeEnum.INPUT_PORT);
						} else {
							dst.setType(TypeEnum.PROCESSOR);
						}
					}
				}
				dto.setDestination(dst);
				
				// Check for input relationships (must be from something that isn't a processgroup)
				if (input.from != null && !input.from.isEmpty() && 
						!HelperYML.isProcessGroup(sourceElement.type) &&
						!HelperYML.ReservedComponents.FUNNEL.isType(sourceElement.type)) {
					dto.setSelectedRelationships(input.from);
				}
				conn.setComponent(dto);
				
				System.out.println(conn);
				processGroupAPI.createConnection(processGroupId, conn);
			}
		}
	}
	
	private void makeFunnel(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		FunnelEntity funnel = new FunnelEntity();
		FunnelDTO dto = new FunnelDTO();
		funnel.setComponent(dto);
		funnel.setRevision(getRevision());
		funnel.setId(ele.id);

		dto.setPosition(position);
		
		FunnelEntity response = processGroupAPI.createFunnel(processGroupId, funnel);

		// Add to tracking to simplify things
		tracker.track(response.getComponent().getParentGroupId(), 
				ele.id, 
				HelperYML.ReservedComponents.FUNNEL.name(),
				response.getComponent().getId());
	}
	
	private void makeProcessGroup(String processGroupId, PositionDTO position, ElementYML ele, Map<String, TemplateYML> templateDB) throws ApiException {
		ProcessGroupEntity pg = new ProcessGroupEntity();
		ProcessGroupDTO dto = new ProcessGroupDTO();
		pg.setComponent(dto);
		pg.setRevision(getRevision());
		pg.setId(ele.id); // NiFi doesn't obey this value for ProcessGroups
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		ProcessGroupEntity newProcessGroup = processGroupAPI.createProcessGroup(processGroupId, pg);
		
		// All additional properties must be set after the initial create
		newProcessGroup.getComponent().setComments(ele.comment);
		newProcessGroup = processGroupAPI.updateProcessGroup(newProcessGroup.getId(), newProcessGroup);
		
		// Add the object to the tracker (track by its name and its YML Id)
		tracker.track(newProcessGroup.getComponent().getParentGroupId(), 
				newProcessGroup.getComponent().getName(), 
				HelperYML.ReservedComponents.PROCESS_GROUP.name(),
				newProcessGroup.getId());
		tracker.track(newProcessGroup.getComponent().getParentGroupId(), 
				ele.id, // The YML id is going to be different from the process Group ID that NiFi creates it as
				HelperYML.ReservedComponents.PROCESS_GROUP.name(),
				newProcessGroup.getId());

		// Fill in the process group with its referenced template after it has been made
		// TODO: you aren't allowed to specify the ID for a process group so we must use the newly assigned one
		//		Research if there is a way to circumvent this so exports are 1 to 1 with imports (possibly versionedComponentId)
		TemplateYML refTemplate = templateDB.get(ele.template);
		importTemplate(newProcessGroup.getId(), refTemplate, templateDB);
	}
	
	private void makeRemoteProcessGroup(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		RemoteProcessGroupEntity rpg = new RemoteProcessGroupEntity();
		RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
		rpg.setComponent(dto);
		rpg.setRevision(getRevision());
		rpg.setId(ele.id);
		
		RemoteProcessGroupContentsDTO contentsDTO = new RemoteProcessGroupContentsDTO();
		dto.setContents(contentsDTO);
		
		RemoteProcessGroupPortDTO portDTO = new RemoteProcessGroupPortDTO();
		portDTO.setName("test port");
		contentsDTO.addInputPortsItem(portDTO);
		
		dto.setPosition(position);
		dto.setTargetUris(ele.properties.get(HelperYML.TARGET_URIS));
		
		RemoteProcessGroupEntity response = processGroupAPI.createRemoteProcessGroup(processGroupId, rpg);
		
		// Add the object to the tracker
		tracker.track(response.getComponent().getParentGroupId(), 
				response.getComponent().getName(), 
				HelperYML.ReservedComponents.REMOTE_PROCESS_GROUP.name(), 
				response.getId());
		tracker.track(response.getComponent().getParentGroupId(), 
				ele.id, 
				HelperYML.ReservedComponents.REMOTE_PROCESS_GROUP.name(), 
				response.getId());
	}
	
	private void makeOutputPort(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		PortEntity response = processGroupAPI.createOutputPort(processGroupId, port);
		
		// Track the newly created object
		tracker.track(response.getComponent().getParentGroupId(),
				response.getComponent().getName(), 
				HelperYML.ReservedComponents.OUTPUT_PORT.name(),
				response.getId());
	}
	
	private void makeInputPort(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);
		
		dto.setPosition(position);
		dto.setName(ele.name);
		
		PortEntity response = processGroupAPI.createInputPort(processGroupId, port);
		
		// Track the newly created object
		tracker.track(response.getComponent().getParentGroupId(),
				response.getComponent().getName(), 
				HelperYML.ReservedComponents.INPUT_PORT.name(),
				response.getId());
	}
	
	private void makeLabel(String processGroupId, PositionDTO position, ElementYML ele) throws ApiException {
		LabelEntity label = new LabelEntity();
		LabelDTO dto = new LabelDTO();
		label.setComponent(dto);
		label.setRevision(getRevision());
		label.setId(ele.id);
		
		dto.setPosition(position);
		dto.setLabel(ele.comment);
		
		LabelEntity response = processGroupAPI.createLabel(processGroupId, label);
		
		// Track the newly created object
		tracker.track(response.getComponent().getParentGroupId(),
				response.getComponent().getLabel(),
				HelperYML.ReservedComponents.LABEL.name(),
				response.getId());
	}

	private void makeProcessor(String processGroupId, ElementYML element, Pair<String,BundleDTO> dependency, PositionDTO position) throws ApiException {
		ProcessorEntity p = new ProcessorEntity();
		ProcessorDTO dto = new ProcessorDTO();
		dto.setConfig(new ProcessorConfigDTO());
		p.setComponent(dto);
		p.setRevision(getRevision());
		
		
		// dto.setId(element.id); Can't specify processor IDs
		dto.setType(dependency.t1);
		dto.setBundle(dependency.t2);
		dto.setName(element.name);
		dto.setPosition(position);
		
		// Update UUID references in properties
		if (element.properties != null) {
			for (String key : element.properties.keySet()) {
				Object val = element.properties.get(key);
				if (val instanceof String) {
					String v = (String) val;
					
					String lookup = tracker.lookupByOldId(v);
					if (lookup != null) {
						element.properties.put(key, lookup);
					}
				}
			}
			
			dto.getConfig().setProperties(element.properties);
		}
		
		System.out.println(element.properties);
		ProcessorEntity response = processGroupAPI.createProcessor(processGroupId, p);
		
		// Track the newly created object (by name and original YML id)
		tracker.track(response.getComponent().getParentGroupId(),
				response.getComponent().getName(),
				element.type,
				response.getId());
		tracker.track(response.getComponent().getParentGroupId(),
				element.id,
				element.type,
				response.getId());
	}

	
	private static Map<String, Pair<String, BundleDTO>> createDependencyLookup(
			Map<String, Map<String, Map<String, Map<String, String>>>> dependencies) {
		Map<String, Pair<String, BundleDTO>> lookup = new HashMap<>();
		
		for (String group : dependencies.keySet()) {
			Map<String, Map<String, Map<String, String>>> artifacts = dependencies.get(group);
			for (String artifact : artifacts.keySet()) {
				 Map<String, Map<String, String>> versions = artifacts.get(artifact);
				 
				 for (String version : versions.keySet()) {
					 Map<String, String> classes = versions.get(version);
					 
					 BundleDTO bundle = new BundleDTO();
					 bundle.setGroup(group);
					 bundle.setArtifact(artifact);
					 bundle.setVersion(version);
					 
					 for (String canonicalName : classes.keySet()) {
						 String fullType = classes.get(canonicalName);
						 
						 lookup.put(canonicalName, new Pair<String, BundleDTO>(fullType, bundle));
					 }
				 }
			}
		}
		
		return lookup;
	}
	
	private List<TemplateYML> loadTemplates(final String importDir) throws JsonParseException, JsonMappingException, IOException {
		List<TemplateYML> templates = new ArrayList<TemplateYML>();
		
		YAMLFactory f = new YAMLFactory();
		f.enable(YAMLGenerator.Feature.MINIMIZE_QUOTES);
		f.disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);
		ObjectMapper mapper = new ObjectMapper(f);
		
		File templateDir = new File(importDir);
		
		for (String template : templateDir.list()) {
			if (template.endsWith(".yaml")) {
				TemplateYML yml = mapper.readValue(new File(templateDir.getAbsolutePath() + File.separator + template), TemplateYML.class);
				templates.add(yml);
			}
		}
		
		return templates;
	}
	
	private RevisionDTO getRevision() {
		RevisionDTO rev = new RevisionDTO();
		rev.setClientId(getClientId());
		rev.setVersion(0L);
		return rev;
	}
	
	// Tester main method
	public static void main(String[] args) {
		BaseCommand.configureApiClients("localhost", "8080", false);
		
		ClearCommand clear = new ClearCommand();
		clear.run();
		
		ImportCommand importCmd = new ImportCommand("./examples/simple/");
		importCmd.run();
	}
}
