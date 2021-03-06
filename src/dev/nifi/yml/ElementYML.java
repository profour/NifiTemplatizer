package dev.nifi.yml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.api.toolkit.model.*;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import dev.nifi.xml.Criteria;
import dev.nifi.xml.Rules;
import dev.nifi.yml.HelperYML.ReservedComponents;

public class ElementYML {

	/**
	 * Human displayed Name for this processor
	 */
	public String name;

	/**
	 * Reference the dependencies for valid type names.
	 * 
	 * Reserved keywords include: Input, Output, ProcessGroup
	 */
	public String type;

	/**
	 * UUID for this processor (not necessary/optional)
	 */
	public String id;

	/**
	 * Template definition to load for this ProcessorGroup
	 */
	public String template;

	/**
	 * Visual position of the processor on the canvas
	 */
	public String position;

	/**
	 * Any comments/labels that have been applied to this component
	 */
	public String comment;

	/**
	 * Only store the list of properties that are different from default
	 */
	public final Map<String, String> properties = new TreeMap<>();

	/**
	 * Only store the aesthetic properties that are different from default
	 */
	public final Map<String, String> styles = new TreeMap<>();

	/**
	 * Scheduling details for this Processor
	 */
	public final Map<String, String> scheduling = new TreeMap<>();

	/**
	 * All incoming connections to this Processor
	 */
	public final List<InputConnectionYML> inputs = new ArrayList<>();
	
	/**
	 * Settings on remote ports
	 */
	public final List<RemotePortYML> remotePorts = new ArrayList<>();

	/**
	 * Advanced rules based on triggers that conditionally generate attributes
	 */
	public RulesYML advanced;

	/*
	 * Private constructor that handles all of the common functionality any visible
	 * element on a canvas will need
	 */
	private ElementYML(String id, String name, String type, PositionDTO position, List<ConnectionEntity> inputs) {
		// set the unique identifiers for this processor
		this.id = id;
		this.name = name;

		if (this.name == null || !this.name.equals(type)) {
			this.type = type;
		}

		handlePosition(position);
		handleConnections(inputs);
	}
	
	/**
	 * Don't use. Only for deserialization.
	 */
	public ElementYML() {}

	/**
	 * Creates a YAML representation for a single ProcessorEntity
	 * 
	 * @param pg                  Processor configuration data from NiFi's API
	 * @param dependencyReference Canonicalized name for this type of Processor
	 * @param inputs              All input connections for this Processor
	 */
	public ElementYML(ProcessorEntity pg, String dependencyReference, List<ConnectionEntity> inputs) {
		this(pg.getId(), pg.getComponent().getName(), dependencyReference, pg.getPosition(), inputs);

		final ProcessorConfigDTO config = pg.getComponent().getConfig();

		// Handle all of the configurable aspects of a Processor and store the deltas
		handleProperties(config.getDescriptors(), config.getProperties());
		handleStyles(pg.getComponent().getStyle());
		handleAnnotations(config.getAnnotationData());

		this.comment = config.getComments();

		// Extract Scheduling details that may have changed for this processor
		String schedulingPeriod = config.getSchedulingPeriod(); // 0 Sec
		String schedulingStrategy = config.getSchedulingStrategy(); // TIMER_DRIVEN
		Integer maxTaskCount = config.getConcurrentlySchedulableTaskCount(); // 1
		String penaltyDuration = config.getPenaltyDuration(); // 30 sec
		String yieldDuration = config.getYieldDuration(); // 1 sec
		Long runDuration = config.getRunDurationMillis(); // 0
		String node = config.getExecutionNode(); // ALL
		String level = config.getBulletinLevel(); // WARN
		
		if (schedulingPeriod != null && !HelperYML.DEFAULT_SCHEDULING_PERIOD.equals(schedulingPeriod)) {
			this.scheduling.put(HelperYML.SCHEDULING_PERIOD, schedulingPeriod);
		}
		if (schedulingStrategy != null && !HelperYML.DEFAULT_SCHEDULING_STRATEGY.equals(schedulingStrategy)) {
			this.scheduling.put(HelperYML.SCHEDULING_STRATEGY, schedulingStrategy);
		}
		if (maxTaskCount != null && HelperYML.DEFAULT_SCHEDULABLE_TASK_COUNT != maxTaskCount) {
			this.scheduling.put(HelperYML.SCHEDULABLE_TASK_COUNT, maxTaskCount.toString());
		}
		if (penaltyDuration != null && !HelperYML.DEFAULT_PENALTY_DURATION.equals(penaltyDuration)) {
			this.scheduling.put(HelperYML.PENALTY_DURATION, penaltyDuration);
		}
		if (yieldDuration != null && !HelperYML.DEFAULT_YIELD_DURATION.equals(yieldDuration)) {
			this.scheduling.put(HelperYML.YIELD_DURATION, yieldDuration);
		}
		if (runDuration != null && HelperYML.DEFAULT_RUN_DURATION != runDuration) {
			this.scheduling.put(HelperYML.RUN_DURATION, runDuration.toString());
		}
		if (node != null && !HelperYML.DEFAULT_EXECUTION_NODE.equals(node)) {
			this.scheduling.put(HelperYML.EXECUTION_NODE, node);
		}
		if (level != null && !HelperYML.DEFAULT_BULLETIN_LEVEL.equals(level)) {
			this.scheduling.put(HelperYML.BULLETIN_LEVEL, level);
		}
		
		// TODO: May want to explicitly mention terminated relationships (rather than implicit from use/disuse)
		// config.getAutoTerminatedRelationships();
	}

	/**
	 * Creates a YAML representation for a single ProcessGroup
	 * 
	 * @param pg           ProcessGroup configuration data from NiFi's API
	 * @param templateName The name of the yaml template that was generate for all
	 *                     of the innards of the ProcessGroup
	 * @param inputs       All input connections for this ProcessGroup
	 */
	public ElementYML(ProcessGroupEntity pg, String templateName, List<ConnectionEntity> inputs) {
		this(pg.getId(), pg.getComponent().getName(), HelperYML.ReservedComponents.PROCESS_GROUP.name(),
				pg.getPosition(), inputs);

		// Reference the template that was generated to handle the innards of the
		// ProcessGroup
		this.template = templateName + ".yaml";
		this.comment = pg.getComponent().getComments();
	}

	/**
	 * Creates a YAML representation for a single Remote ProcessGroup
	 * @param rpg RemoteProcessGroup configuration data from NiFi's API
	 * @param list All input connections for this RemoteProcessGroup
	 */
	public ElementYML(RemoteProcessGroupEntity rpg, List<ConnectionEntity> inputs) {
		this(rpg.getId(), rpg.getComponent().getName(), HelperYML.ReservedComponents.REMOTE_PROCESS_GROUP.name(),
				rpg.getPosition(), inputs);
		
		RemoteProcessGroupDTO config = rpg.getComponent();
		this.comment = config.getComments();
		
		// Remote Hosts hosting the RemoteProcessGroup
		if (config.getTargetUris() != null && !config.getTargetUris().isEmpty()) {
			this.properties.put(HelperYML.TARGET_URIS, config.getTargetUris());
		}
		
		// Proxy settings
		if (config.getProxyHost() != null && !config.getProxyHost().isEmpty()) {
			this.properties.put(HelperYML.PROXY_HOST, config.getProxyHost());
		}
		if (config.getProxyPort() != null) {
			this.properties.put(HelperYML.PROXY_PORT, config.getProxyPort().toString());
		}
		if (config.getProxyUser() != null && !config.getProxyUser().isEmpty()) {
			this.properties.put(HelperYML.PROXY_USER, config.getProxyUser());
		}
		if (config.getProxyPassword() != null && !config.getProxyPassword().isEmpty()) {
			this.properties.put(HelperYML.PROXY_PASSWORD, config.getProxyPassword());
		}
		
		// General Network settings
		if (config.getLocalNetworkInterface() != null && !config.getLocalNetworkInterface().isEmpty()) {
			this.properties.put(HelperYML.NETWORK, config.getLocalNetworkInterface());
		}
		if (!HelperYML.DEFAULT_REMOTE_TRANSPORT.equals(config.getTransportProtocol())) {
			this.properties.put(HelperYML.PROTOCOL, config.getTransportProtocol());
		}
		if (!HelperYML.DEFAULT_REMOTE_TIMEOUT.equals(config.getCommunicationsTimeout())) {
			this.properties.put(HelperYML.TIMEOUT, config.getCommunicationsTimeout());
		}
		if (!HelperYML.DEFAULT_REMOTE_YIELD.equals(config.getYieldDuration())) {
			this.properties.put(HelperYML.YIELD_DURATION, config.getYieldDuration());
		}
		
		// Remote port configuration settings
		RemoteProcessGroupContentsDTO remotePortConfigs = config.getContents();
		for (RemoteProcessGroupPortDTO port : remotePortConfigs.getInputPorts()) {
			remotePorts.add(new RemotePortYML(port, ReservedComponents.INPUT_PORT));
		}
		for (RemoteProcessGroupPortDTO port : remotePortConfigs.getOutputPorts()) {
			remotePorts.add(new RemotePortYML(port, ReservedComponents.OUTPUT_PORT));
		}
	}

	/**
	 * Creates a YAML representation for a single input/output port
	 * 
	 * @param port   Port configuration data from NiFi's API
	 * @param inputs All input connections for this port
	 */
	public ElementYML(PortEntity port, List<ConnectionEntity> inputs) {
		this(port.getId(), port.getComponent().getName(), port.getPortType(), port.getPosition(), inputs);
		
		this.comment = port.getComponent().getComments();
	}

	/**
	 * Creates a YAML representation for a single funnel
	 * 
	 * @param f      Funnel configuration data from NiFi's API
	 * @param inputs All input connections for this funnel
	 */
	public ElementYML(FunnelEntity f, List<ConnectionEntity> inputs) {
		this(f.getId(), null, HelperYML.ReservedComponents.FUNNEL.name(), f.getPosition(), inputs);
	}

	/**
	 * Creates a YAML representation for a label
	 * 
	 * @param l Label configuration data from NiFi's API
	 */
	public ElementYML(LabelEntity l) {
		this(l.getId(), null, HelperYML.ReservedComponents.LABEL.name(), l.getPosition(), null);

		this.comment = l.getComponent().getLabel();

		// Coerce width/height into the styles bucket
		this.styles.put(HelperYML.WIDTH, HelperYML.formatDoubleTruncated(l.getComponent().getWidth()));
		this.styles.put(HelperYML.HEIGHT, HelperYML.formatDoubleTruncated(l.getComponent().getHeight()));

		handleStyles(l.getComponent().getStyle());
	}
	
	public String getType() {
		return (this.type != null) ? type : name;
	}

	/*
	 * Helper method that checks all properties and looks for any configures values
	 * that differ from the default value.
	 */
	private void handleProperties(Map<String, PropertyDescriptorDTO> defaultProperties,
			Map<String, String> configuredValues) {

		// Populate the list of properties that have changed (compare default value vs
		// configured value)
		for (String propertyName : defaultProperties.keySet()) {
			PropertyDescriptorDTO defaultProperty = defaultProperties.get(propertyName);
			String configuredValue = configuredValues.get(propertyName);

			// Check if the configuredValue differs from the default value
			if ((defaultProperty.getDefaultValue() == null && configuredValue != null)
					|| (defaultProperty.getDefaultValue() != null
							&& !defaultProperty.getDefaultValue().equals(configuredValue))) {
				this.properties.put(propertyName, configuredValue);
			}
		}
	}

	private void handleStyles(Map<String, String> styles) {

		// Look at all styles and check if any are non-default values
		for (String key : styles.keySet()) {
			String value = styles.get(key);
			
			// Check if the key-value pairs are simply the default values
			// Continue on to the next key-value pair if they are the defaults
			switch (key) {
			case HelperYML.FONT_SIZE:
				if (HelperYML.DEFAULT_STYLE_FONT_SIZE.equals(value)) {
					continue;
				}
				break;
			case HelperYML.BG_COLOR:
				if (HelperYML.DEFAULT_STYLE_COLOR.equals(value)) {
					continue;
				}
				break;
			}
			
			// Only store the value if it is different from the default
			this.styles.put(key, value);
		}
	}

	/*
	 * Helper method to store all input connections to a processor
	 */
	private void handleConnections(List<ConnectionEntity> inputs) {
		if (inputs != null) {
			// Process incoming connections to fill out the input listing
			for (ConnectionEntity connection : inputs) {
				this.inputs.add(new InputConnectionYML(connection));
			}
		}
	}

	/*
	 * Helper method to create a consistent storage format for positional data
	 */
	private void handlePosition(PositionDTO position) {
		// Truncate position down to integer values
		this.position = HelperYML.formatPosition(position.getX(), position.getY());
	}

	/*
	 * Helper method to convert XML annotation data into a storable YAML format.
	 * This only applies to the advanced tab on a processor where
	 * Rules/Conditions/Actions are configured.
	 */
	private void handleAnnotations(String xmlAnnotations) {
		// Check to see if there are any annotations, if not just bail out early
		if (xmlAnnotations == null || xmlAnnotations.isEmpty()) {
			return;
		}

		List<RuleYML> rules = new ArrayList<RuleYML>();

		XmlMapper xmlMapper = new XmlMapper();
		try {
			Criteria criteria = xmlMapper.readValue(xmlAnnotations, Criteria.class);

			for (Rules rule : criteria.rules) {
				RuleYML r = new RuleYML(rule.name, rule.conditions, rule.actions);
				rules.add(r);
			}

			this.advanced = new RulesYML(criteria.flowFilePolicy, rules);

		} catch (IOException e) {
			// TODO: setup proper logging
			System.out.println("Failed to parse xml: " + xmlAnnotations);
			e.printStackTrace();
		}
	}

}
