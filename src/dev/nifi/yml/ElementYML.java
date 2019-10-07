package dev.nifi.yml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.FunnelEntity;
import org.apache.nifi.api.toolkit.model.LabelEntity;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessorConfigDTO;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.PropertyDescriptorDTO;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupContentsDTO;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupDTO;
import org.apache.nifi.api.toolkit.model.RemoteProcessGroupEntity;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import dev.nifi.xml.Criteria;
import dev.nifi.xml.Rules;

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
	public Map<String, String> properties;

	/**
	 * Only store the aesthetic properties that are different from default
	 */
	public Map<String, String> styles;

	/**
	 * Scheduling details for this Processor
	 */
	public Map<String, String> scheduling;

	/**
	 * All incoming connections to this Processor
	 */
	public List<InputConnectionYML> inputs;

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
		// TODO:
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

		this.properties = new TreeMap<>();
		this.properties.put("targetUris", config.getTargetUris());
		
		// Proxy settings
		this.properties.put("proxyHost", config.getProxyHost());
		if (config.getProxyPort() != null) {
			this.properties.put("proxyPort", config.getProxyPort().toString());
		}
		this.properties.put("proxyUser", config.getProxyUser());
		this.properties.put("proxyPassword", config.getProxyPassword());
		
		this.properties.put("network", config.getLocalNetworkInterface());
		if (!HelperYML.DEFAULT_REMOTE_TRANSPORT.equals(config.getTransportProtocol())) {
			this.properties.put("protocol", config.getTransportProtocol());
		}
		if (!HelperYML.DEFAULT_REMOTE_TIMEOUT.equals(config.getCommunicationsTimeout())) {
			this.properties.put("timeout", config.getCommunicationsTimeout());
		}
		if (!HelperYML.DEFAULT_REMOTE_YIELD.equals(config.getYieldDuration())) {
			this.properties.put("yieldDuration", config.getYieldDuration());
		}
		
		// TODO: It is possible to configure settings on the remote ports of a RemoteProcessGroup
		// RemoteProcessGroupContentsDTO remotePortConfigs = config.getContents();
	}

	/**
	 * Creates a YAML representation for a single input/output port
	 * 
	 * @param port   Port configuration data from NiFi's API
	 * @param inputs All input connections for this port
	 */
	public ElementYML(PortEntity port, List<ConnectionEntity> inputs) {
		this(port.getId(), port.getComponent().getName(), port.getPortType(), port.getPosition(), inputs);
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

		this.styles = new TreeMap<String, String>();

		// Coerce width/height into the styles bucket
		this.styles.put(HelperYML.WIDTH, String.format("%.0f", l.getComponent().getWidth()));
		this.styles.put(HelperYML.HEIGHT, String.format("%.0f", l.getComponent().getHeight()));

		handleStyles(l.getComponent().getStyle());
	}

	/*
	 * Helper method that checks all properties and looks for any configures values
	 * that differ from the default value.
	 */
	private void handleProperties(Map<String, PropertyDescriptorDTO> defaultProperties,
			Map<String, String> configuredValues) {
		this.properties = new TreeMap<String, String>();

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
		if (this.styles == null) {
			this.styles = new TreeMap<>();
		}

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
			this.inputs = new ArrayList<>();

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
		this.position = String.format("%.0f,%.0f", position.getX(), position.getY());
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
				RuleYML r = new RuleYML(rule.conditions, rule.actions);
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
