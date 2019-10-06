package dev.nifi.yml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.FunnelEntity;
import org.apache.nifi.api.toolkit.model.PortEntity;
import org.apache.nifi.api.toolkit.model.PositionDTO;
import org.apache.nifi.api.toolkit.model.ProcessGroupEntity;
import org.apache.nifi.api.toolkit.model.ProcessorConfigDTO;
import org.apache.nifi.api.toolkit.model.ProcessorEntity;
import org.apache.nifi.api.toolkit.model.PropertyDescriptorDTO;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import dev.nifi.xml.Criteria;
import dev.nifi.xml.Rules;

public class ProcessorYML {

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
	 * Advanced rules based on triggers that conditionally generate attributes
	 */
	public List<RulesYML> rules;

	/**
	 * Policy of how FlowFiles are handled when multiple rules are triggered
	 */
	public String rulesPolicy;

	/**
	 * Scheduling details for this Processor
	 */
	public Map<String, String> scheduling;

	/**
	 * All incoming connections to this Processor
	 */
	public List<InputConnectionYML> inputs;

	/*
	 * Private constructor that handles all of the common functionality any visible
	 * element on a canvas will need
	 */
	private ProcessorYML(String id, String name, String type, PositionDTO position, List<ConnectionEntity> inputs) {
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
	public ProcessorYML(ProcessorEntity pg, String dependencyReference, List<ConnectionEntity> inputs) {
		this(pg.getId(), pg.getComponent().getName(), dependencyReference, pg.getPosition(), inputs);

		final ProcessorConfigDTO config = pg.getComponent().getConfig();

		// Handle all of the configurable aspects of a Processor and store the deltas
		handleProperties(config.getDescriptors(), config.getProperties());
		handleAnnotations(config.getAnnotationData());

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
	public ProcessorYML(ProcessGroupEntity pg, String templateName, List<ConnectionEntity> inputs) {
		this(pg.getId(), pg.getComponent().getName(), ReservedComponents.PROCESS_GROUP.name(), pg.getPosition(),
				inputs);

		// Reference the template that was generated to handle the innards of the
		// ProcessGroup
		this.template = templateName + ".yaml";
		this.comment = pg.getComponent().getComments();
	}

	/**
	 * Creates a YAML representation for a single input/output port
	 * 
	 * @param port   Port configuration data from NiFi's API
	 * @param inputs All input connections for this port
	 */
	public ProcessorYML(PortEntity port, List<ConnectionEntity> inputs) {
		this(port.getId(), port.getComponent().getName(), port.getPortType(), port.getPosition(), inputs);
	}

	/**
	 * Creates a YAML representation for a single funnel
	 * 
	 * @param f      Funnel configuration data from NiFi's API
	 * @param inputs All input connections for this funnel
	 */
	public ProcessorYML(FunnelEntity f, List<ConnectionEntity> inputs) {
		this(f.getId(), null, ReservedComponents.FUNNEL.name(), f.getPosition(), inputs);
	}

	/*
	 * Helper method that checks all properties and looks for any configures values
	 * that differ from the default value.
	 */
	private void handleProperties(Map<String, PropertyDescriptorDTO> defaultProperties,
			Map<String, String> configuredValues) {
		this.properties = new HashMap<String, String>();

		// Populate the list of properties that have changed (compare default value vs
		// configured value)
		for (String propertyName : defaultProperties.keySet()) {
			PropertyDescriptorDTO defaultProperty = defaultProperties.get(propertyName);
			String configuredValue = configuredValues.get(propertyName);

			// Check if the configuredValue differs from the default value
			if ((defaultProperty.getDefaultValue() == null && configuredValue != null)
					|| (defaultProperty.getDefaultValue() != null && !defaultProperty.getDefaultValue().equals(configuredValue))) {
				this.properties.put(propertyName, configuredValue);
			}
		}
	}

	/*
	 * Helper method to store all input connections to a processor
	 */
	private void handleConnections(List<ConnectionEntity> inputs) {

		// Process incoming connections to fill out the input listing
		this.inputs = new ArrayList<>();
		if (inputs != null) {
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

		this.rules = new ArrayList<>();

		XmlMapper xmlMapper = new XmlMapper();
		try {
			Criteria criteria = xmlMapper.readValue(xmlAnnotations, Criteria.class);

			this.rulesPolicy = criteria.flowFilePolicy;

			for (Rules rule : criteria.rules) {
				RulesYML r = new RulesYML(rule.conditions, rule.actions);

				this.rules.add(r);
			}

		} catch (IOException e) {
			// TODO: setup proper logging
			System.out.println("Failed to parse xml: " + xmlAnnotations);
			e.printStackTrace();
		}
	}

}
