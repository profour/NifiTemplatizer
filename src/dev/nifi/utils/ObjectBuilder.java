package dev.nifi.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;

import org.apache.nifi.api.toolkit.ApiClient;
import org.apache.nifi.api.toolkit.ApiException;
import org.apache.nifi.api.toolkit.api.ProcessGroupsApi;
import org.apache.nifi.api.toolkit.model.*;
import org.apache.nifi.api.toolkit.model.ConnectableDTO.TypeEnum;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import dev.nifi.xml.Actions;
import dev.nifi.xml.Conditions;
import dev.nifi.xml.Criteria;
import dev.nifi.xml.Rules;
import dev.nifi.yml.ControllerYML;
import dev.nifi.yml.ElementYML;
import dev.nifi.yml.HelperYML;
import dev.nifi.yml.HelperYML.ReservedComponents;
import dev.nifi.yml.InputConnectionYML;
import dev.nifi.yml.RuleYML;
import dev.nifi.yml.RulesYML;
import dev.nifi.yml.TemplateYML;

public class ObjectBuilder {

	private final String clientId;
	private final ProcessGroupsApi processGroupAPI;
	
	// Stack of ProcessGroups that are being dealt with and the variables that are local to that stack only
	private final Stack<ProcessGroupStackElement> processGroupStack = new Stack<ProcessGroupStackElement>();
	
	// Global object tracker
	private final ObjectTracker tracker = new ObjectTracker();
	
	public ObjectBuilder(ApiClient apiClient, String clientId) {
		this.clientId = clientId;
		
		processGroupAPI = new ProcessGroupsApi(apiClient);
	}
	
	public void enterProcessGroup(String processGroupId) {
		this.processGroupStack.push(new ProcessGroupStackElement(processGroupId));
	}
	
	public String leaveProcessGroup() {
		return this.processGroupStack.pop().id;
	}
	
	public void setDependenciesLookup(Map<String, Pair<String, BundleDTO>> depLookup) {
		this.processGroupStack.peek().dependencies = depLookup;
	}
	
	public String getProcessGroupId() {
		return processGroupStack.peek().id;
	}
	
	public Pair<String, BundleDTO> lookup(String canonicalName) {
		return processGroupStack.peek().dependencies.get(canonicalName);
	}

	public String getNewId(String id) {
		return tracker.lookupByOldId(id);
	}
	
	public ControllerServiceEntity makeControllerService(ControllerYML controller) throws ApiException {
		ControllerServiceEntity cont = new ControllerServiceEntity();
		ControllerServiceDTO dto = new ControllerServiceDTO();
		cont.setComponent(dto);
		cont.setRevision(getRevision());
		
		// Lookup the typing for this based on the canonical type
		Pair<String, BundleDTO> dep = this.lookup(controller.getType());
		dto.setBundle(dep.t2);
		dto.setType(dep.t1);
		

		dto.setName(controller.name);
		
		if (controller.properties != null) {
			Map<String, String> props = new HashMap<String, String>();
			for (String key : controller.properties.keySet()) {
				props.put(key, controller.properties.get(key).toString());
			}
			dto.setProperties(props);
		}
		
		ControllerServiceEntity response = processGroupAPI.createControllerService(getProcessGroupId(), cont);
		
		// Track the newly created controller (old id -> new id)
		tracker.track(controller.id, response.getId());
		
		return response;
	}
	
	public FunnelEntity makeFunnel(ElementYML ele) throws ApiException {
		FunnelEntity funnel = new FunnelEntity();
		FunnelDTO dto = new FunnelDTO();
		funnel.setComponent(dto);
		funnel.setRevision(getRevision());

		PositionDTO position = HelperYML.createPosition(ele.position);
		dto.setPosition(position);
		
		FunnelEntity response = processGroupAPI.createFunnel(getProcessGroupId(), funnel);

		// Track the newly created funnel (old id -> new id)
		tracker.track(ele.id, response.getId());
		
		return response;
	}
	
	public ProcessGroupEntity makeProcessGroup(ElementYML ele, Map<String, TemplateYML> templateDB) throws ApiException {
		ProcessGroupEntity pg = new ProcessGroupEntity();
		ProcessGroupDTO dto = new ProcessGroupDTO();
		pg.setComponent(dto);
		pg.setRevision(getRevision());

		PositionDTO position = HelperYML.createPosition(ele.position);
		dto.setPosition(position);
		dto.setName(ele.name);
		
		ProcessGroupEntity newProcessGroup = processGroupAPI.createProcessGroup(getProcessGroupId(), pg);
		
		// All additional properties must be set after the initial create
		newProcessGroup.getComponent().setComments(ele.comment);
		ProcessGroupEntity response = processGroupAPI.updateProcessGroup(newProcessGroup.getId(), newProcessGroup);

		// Track the newly created processgroup (old id -> new id)
		tracker.track(ele.id, response.getId());
		
		return newProcessGroup;
	}
	
	public RemoteProcessGroupEntity makeRemoteProcessGroup(ElementYML ele) throws ApiException {
		RemoteProcessGroupEntity rpg = new RemoteProcessGroupEntity();
		RemoteProcessGroupDTO dto = new RemoteProcessGroupDTO();
		rpg.setComponent(dto);
		rpg.setRevision(getRevision());
		
		RemoteProcessGroupContentsDTO contentsDTO = new RemoteProcessGroupContentsDTO();
		dto.setContents(contentsDTO);

		PositionDTO position = HelperYML.createPosition(ele.position);
		dto.setPosition(position);
		dto.setTargetUris(ele.properties.get(HelperYML.TARGET_URIS));
		
		RemoteProcessGroupEntity response = processGroupAPI.createRemoteProcessGroup(getProcessGroupId(), rpg);
		
		// Track the newly created remote process group (old id -> new id)
		tracker.track(ele.id, response.getId());
		
		return response;
	}
	
	public PortEntity makeOutputPort(ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);

		PositionDTO position = HelperYML.createPosition(ele.position);
		dto.setPosition(position);
		dto.setName(ele.name);
		dto.setComments(ele.comment);
		
		PortEntity response = processGroupAPI.createOutputPort(getProcessGroupId(), port);
		
		// Track the newly created output port by id as well as port name
		tracker.track(ele.id, response.getId());
		tracker.track(getProcessGroupId(), ele.name, ele.type, response.getId());
		
		return response;
	}
	
	public PortEntity makeInputPort(ElementYML ele) throws ApiException {
		PortEntity port = new PortEntity();
		PortDTO dto = new PortDTO();
		port.setComponent(dto);
		port.setRevision(getRevision());
		port.setId(ele.id);

		PositionDTO position = HelperYML.createPosition(ele.position);
		dto.setPosition(position);
		dto.setName(ele.name);
		dto.setComments(ele.comment);
		
		PortEntity response = processGroupAPI.createInputPort(getProcessGroupId(), port);

		// Track the newly created input port by id as well as port name
		tracker.track(ele.id, response.getId());
		tracker.track(getProcessGroupId(), ele.name, ele.type, response.getId());
		
		return response;
	}
	
	public LabelEntity makeLabel(ElementYML ele) throws ApiException {
		LabelEntity label = new LabelEntity();
		LabelDTO dto = new LabelDTO();
		label.setComponent(dto);
		label.setRevision(getRevision());
		label.setId(ele.id);

		PositionDTO position = HelperYML.createPosition(ele.position);
		dto.setPosition(position);
		dto.setLabel(ele.comment);

		if (ele.styles != null) {
			if (ele.styles.containsKey(HelperYML.WIDTH)) {
				dto.setWidth(Double.parseDouble(ele.styles.get(HelperYML.WIDTH)));
			}
			if (ele.styles.containsKey(HelperYML.HEIGHT)) {
				dto.setHeight(Double.parseDouble(ele.styles.get(HelperYML.HEIGHT)));
				
			}
			dto.setStyle(ele.styles);
		}
		
		LabelEntity response = processGroupAPI.createLabel(getProcessGroupId(), label);
		
		// Track the newly created label (old id -> new id)
		tracker.track(ele.id, response.getId());
		
		return response;
	}

	public ProcessorEntity makeProcessor(ElementYML element) throws ApiException {
		ProcessorEntity p = new ProcessorEntity();
		ProcessorDTO dto = new ProcessorDTO();
		dto.setConfig(new ProcessorConfigDTO());
		p.setComponent(dto);
		p.setRevision(getRevision());
		
		
		// dto.setId(element.id); Can't specify processor IDs
		Pair<String, BundleDTO> dependency = this.lookup(element.getType());
		dto.setType(dependency.t1);
		dto.setBundle(dependency.t2);
		
		dto.setName(element.name);
		dto.setPosition(HelperYML.createPosition(element.position));
		dto.setStyle(element.styles);
		
		// Set any properties that may have changed from default
		if (element.properties != null) {
			for (String key : element.properties.keySet()) {
				Object val = element.properties.get(key);
				if (val instanceof String) {
					String v = (String) val;
					
					// Update UUID references in properties
					String lookup = tracker.lookupByOldId(v);
					if (lookup != null) {
						element.properties.put(key, lookup);
					}
				}
			}
			
			dto.getConfig().setProperties(element.properties);
		}
		
		// Check if there are any scheduling properties that need to be assigned
		if (element.scheduling != null && !element.scheduling.isEmpty()) {

			String schedulingPeriod = element.scheduling.get(HelperYML.SCHEDULING_PERIOD);
			String schedulingStrategy = element.scheduling.get(HelperYML.SCHEDULING_STRATEGY);
			String maxTasks = element.scheduling.get(HelperYML.SCHEDULABLE_TASK_COUNT); // Integer
			String penaltyDuration = element.scheduling.get(HelperYML.PENALTY_DURATION);
			String yieldDuration = element.scheduling.get(HelperYML.YIELD_DURATION);
			String runDuration = element.scheduling.get(HelperYML.RUN_DURATION); // Long
			String executionNode = element.scheduling.get(HelperYML.EXECUTION_NODE);
			String bulletinLevel = element.scheduling.get(HelperYML.BULLETIN_LEVEL);
			
			if (schedulingPeriod != null && !schedulingPeriod.isEmpty()) {
				dto.getConfig().setSchedulingPeriod(schedulingPeriod);
			}
			if (schedulingStrategy != null && !schedulingStrategy.isEmpty()) {
				dto.getConfig().setSchedulingStrategy(schedulingStrategy);
			}
			if (maxTasks != null && !maxTasks.isEmpty()) {
				dto.getConfig().setConcurrentlySchedulableTaskCount(Integer.parseInt(maxTasks));
			}
			if (penaltyDuration != null && !penaltyDuration.isEmpty()) {
				dto.getConfig().setPenaltyDuration(penaltyDuration);
			}
			if (yieldDuration != null && !yieldDuration.isEmpty()) {
				dto.getConfig().setYieldDuration(yieldDuration);
			}
			if (runDuration != null && !runDuration.isEmpty()) {
				dto.getConfig().setRunDurationMillis(Long.parseLong(runDuration));
			}
			if (executionNode != null && !executionNode.isEmpty()) {
				dto.getConfig().setExecutionNode(executionNode);
			}
			if (bulletinLevel != null && !bulletinLevel.isEmpty()) {
				dto.getConfig().setBulletinLevel(bulletinLevel);
			}
		}
		
		// Check if there is any annotation data (advanced rules)
		if (element.advanced != null) {
			String annotationData = makeAnnotationData(element.advanced);
			
			dto.getConfig().setAnnotationData(annotationData);
		}
		
		ProcessorEntity response = processGroupAPI.createProcessor(getProcessGroupId(), p);
		
		// Track the newly created processor (old id -> new id)
		tracker.track(element.id, response.getId());
		
		return response;
	}
	
	public ConnectionEntity makeConnection(ElementYML sourceElement, ElementYML destinationElement, InputConnectionYML input) throws ApiException {
		
		ConnectionEntity conn = new ConnectionEntity();
		conn.setRevision(getRevision());
		
		ConnectionDTO dto = new ConnectionDTO();
		
		// Set the source & destination of the connection
		ConnectableDTO src = makeSourceConnectable(sourceElement, input.from);
		ConnectableDTO dst = makeDestinationConnectable(destinationElement, input.to);

		// If we weren't able to make src or dst connections, something bad happened (or a remote process didn't detect remote ports yet)
		if (src == null || dst == null) {
			// TODO: add proper logging
			return null;
		}
		dto.setSource(src);
		dto.setDestination(dst);
		
		// Check for input relationships (must be from something that isn't a processgroup)
		if (input.from != null && !input.from.isEmpty() && 
				!HelperYML.isProcessGroup(sourceElement.type) &&
				!ReservedComponents.FUNNEL.isType(sourceElement.type)) {
			dto.setSelectedRelationships(input.from);
		}
		conn.setComponent(dto);
		
		return processGroupAPI.createConnection(getProcessGroupId(), conn);
	}
	
	private ConnectableDTO makeSourceConnectable(ElementYML source, List<String> from) {

		// Types of source connections:
		//  - Port -> *
		//  - ProcessGroup(Port) -> *
		//  - RemoteProcessGroup(Remote Port) -> *
		//  - Processor/Funnel -> *

		String groupId = null;
		String id = null;
		TypeEnum type;
		
		// Determine the new GroupId and ID of the destination element
		if (HelperYML.isProcessGroup(source.type)) {
			groupId = tracker.lookupByOldId(source.id);
			id = tracker.getIdForObject(groupId, from.get(0), ReservedComponents.OUTPUT_PORT);
		} else {
			groupId = getProcessGroupId();
			id = tracker.lookupByOldId(source.id);
		}
		
		// Determine the true type of the destination element
		if (HelperYML.isPort(source.type)) {
			type = TypeEnum.valueOf(source.type);
		} else if (ReservedComponents.PROCESS_GROUP.isType(source.type)) {
			type = TypeEnum.OUTPUT_PORT;
		} else if (ReservedComponents.REMOTE_PROCESS_GROUP.isType(source.type)) {
			type = TypeEnum.REMOTE_OUTPUT_PORT;
			return null;
		} else {
			try {
				type = TypeEnum.valueOf(source.type);
			} catch (Exception e) {
				type = TypeEnum.PROCESSOR;
			}
		}

		ConnectableDTO connectable = new ConnectableDTO();
		connectable.setGroupId(groupId);
		connectable.setId(id);
		connectable.setType(type);
		
		return connectable;
	}
	
	private ConnectableDTO makeDestinationConnectable(ElementYML destination, String to) {
		// Types of destination connections:
		//  - * -> Port
		//  - * -> ProcessGroup(Port)
		//  - * -> RemoteProcessGroup(Remote Port)
		//  - * -> Processor/Funnel
		String groupId = null;
		String id = null;
		TypeEnum type;
		
		// Determine the new GroupId and ID of the destination element
		if (HelperYML.isProcessGroup(destination.type)) {
			groupId = tracker.lookupByOldId(destination.id);
			id = tracker.getIdForObject(groupId, to, ReservedComponents.INPUT_PORT);
		} else {
			groupId = getProcessGroupId();
			id = tracker.lookupByOldId(destination.id);
		}
		
		// Determine the true type of the destination element
		if (HelperYML.isPort(destination.type)) {
			type = TypeEnum.valueOf(destination.type);
		} else if (ReservedComponents.PROCESS_GROUP.isType(destination.type)) {
			type = TypeEnum.INPUT_PORT;
		} else if (ReservedComponents.REMOTE_PROCESS_GROUP.isType(destination.type)) {
			type = TypeEnum.REMOTE_INPUT_PORT;
			return null;
		} else {
			try {
				type = TypeEnum.valueOf(destination.type);
			} catch (Exception e) {
				type = TypeEnum.PROCESSOR;
			}
		}

		ConnectableDTO connectable = new ConnectableDTO();
		connectable.setGroupId(groupId);
		connectable.setId(id);
		connectable.setType(type);
		
		return connectable;
	}
	
	private String makeAnnotationData(RulesYML rules) {
		XmlMapper xmlMapper = new XmlMapper();
		
		// Reconstruct the original criteria object from the YML formatted rules
		Criteria criteria = new Criteria();
		
		// initialize top level values
		criteria.rules = new ArrayList<>();
		criteria.flowFilePolicy = rules.policy;
		
		// Iterate over all yml Rules and convert them to the XML DTO class format
		// NOTE: Generating the ID's is important, otherwise the API has issues with the data
		for (RuleYML rule : rules.rules) {
			Rules newRule = new Rules();
			
			newRule.conditions = new ArrayList<>();
			newRule.actions = new ArrayList<>();
			newRule.name = rule.name;
			newRule.id = UUID.randomUUID().toString();

			if (rule.conditions != null) {
				for (String condition : rule.conditions) {
					Conditions newCondition = new Conditions();
					newCondition.expression = condition;
					newCondition.id = UUID.randomUUID().toString();
					newRule.conditions.add(newCondition);
				}
			}
			if (rule.actions != null) {
				for (String key : rule.actions.keySet()) {
					Actions newAction = new Actions();
					newAction.attribute = key;
					newAction.value = rule.actions.get(key);
					newAction.id = UUID.randomUUID().toString();
					newRule.actions.add(newAction);
				}
			}
			
			criteria.rules.add(newRule);
		}
		
		// Attempt to write the data out to XML
		try {
			return xmlMapper.writeValueAsString(criteria);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	private RevisionDTO getRevision() {
		RevisionDTO rev = new RevisionDTO();
		rev.setClientId(clientId);
		rev.setVersion(0L);
		return rev;
	}
	
	private class ProcessGroupStackElement {

		public final String id;
		
		// Canonical type name -> (Type, Bundle)
		public Map<String, Pair<String, BundleDTO>> dependencies;	

		public ProcessGroupStackElement(String processGroupId) {
			this.id = processGroupId;
		}
	}
}
