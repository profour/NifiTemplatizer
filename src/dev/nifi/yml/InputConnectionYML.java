package dev.nifi.yml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.api.toolkit.model.ConnectableDTO.TypeEnum;
import org.apache.nifi.api.toolkit.model.ConnectionDTO;
import org.apache.nifi.api.toolkit.model.ConnectionDTO.LoadBalanceCompressionEnum;
import org.apache.nifi.api.toolkit.model.ConnectionDTO.LoadBalanceStrategyEnum;
import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.ConnectionEntity.SourceTypeEnum;
import org.apache.nifi.api.toolkit.model.PositionDTO;

public class InputConnectionYML {

	/**
	 * Source processor flowfiles are coming from
	 */
	public String source;

	/**
	 * Relationship(s) that are coming from the source processor
	 */
	public List<String> relationships;

	/**
	 * If the source is a process group, specify the Output Port name
	 */
	public String fromPort;

	/**
	 * If the destination is a process group, specify the Input Port to receive the
	 * data on
	 */
	public String toPort;

	/**
	 * Special properties for the incoming connection (queuing type, queue size,
	 * etc)
	 */
	public Map<String, Object> properties;

	/**
	 * Aesthetic positioning of bends in the connection link between processors
	 */
	public List<String> position;
	
	/**
	 * Only for Jackson deserialization
	 */
	public InputConnectionYML() {}

	public InputConnectionYML(ConnectionEntity connection) {
		this.properties = new TreeMap<>();

		// If the source is a REMOTE_OUTPUT_PORT, we should reference the (remote) group
		// id, rather than the regular id
		this.source = connection.getSourceType() == SourceTypeEnum.REMOTE_OUTPUT_PORT ||
				      connection.getSourceType() == SourceTypeEnum.OUTPUT_PORT 
				      ? connection.getSourceGroupId() : connection.getSourceId();

		ConnectionDTO conn = connection.getComponent();

		this.relationships = conn.getSelectedRelationships();

		// If our input is coming from another process group, 
		// we need to identify the port name
		if (conn.getSource().getType() == TypeEnum.OUTPUT_PORT
				|| conn.getSource().getType() == TypeEnum.REMOTE_OUTPUT_PORT
				|| conn.getSource().getType() == TypeEnum.FUNNEL) {
			this.fromPort = conn.getSource().getName();
		}
		
		// If this is connecting to a process group, need to map the input onto
		// the correct input port inside the process group
		if (conn.getDestination().getType() == TypeEnum.INPUT_PORT
				|| conn.getDestination().getType() == TypeEnum.REMOTE_INPUT_PORT) {
			this.toPort = conn.getDestination().getName();
		}

		// See if any manual position of the link was performed (aesthetic only)
		if (!conn.getBends().isEmpty()) {
			this.position = new ArrayList<String>();
			for (PositionDTO pos : conn.getBends()) {
				// Truncate positioning of bends to be integer values only
				this.position.add(String.format("%.0f,%.0f", pos.getX(), pos.getY()));
			}
		}

		// Try to determine if any link properties have changed from their defaults
		if (conn.getName() != null && !conn.getName().isEmpty()) {
			this.properties.put("name", conn.getName());
		}
		if (conn.getBackPressureObjectThreshold() != 10000) {
			this.properties.put("backPressureObjectThreshold", conn.getBackPressureObjectThreshold().toString());
		}
		if (!conn.getBackPressureDataSizeThreshold().equals("1 GB")) {
			this.properties.put("backPressureDataSizeThreshold", conn.getBackPressureDataSizeThreshold());
		}
		if (conn.getLoadBalanceStrategy() != LoadBalanceStrategyEnum.DO_NOT_LOAD_BALANCE) {
			this.properties.put("loadBalanceStrategy", conn.getLoadBalanceStrategy().name());
		}
		if (conn.getLoadBalancePartitionAttribute() != null && !conn.getLoadBalancePartitionAttribute().isEmpty()) {
			this.properties.put("loadBalancePartitionAttribute", conn.getLoadBalancePartitionAttribute());
		}
		if (conn.getLoadBalanceCompression() != LoadBalanceCompressionEnum.DO_NOT_COMPRESS) {
			this.properties.put("loadBalanceCompression", conn.getLoadBalanceCompression().name());
		}
		if (!conn.getFlowFileExpiration().equals("0 sec")) {
			this.properties.put("flowFileExpiration", conn.getFlowFileExpiration());
		}
		if (!conn.getPrioritizers().isEmpty()) {
			this.properties.put("prioritizers", conn.getPrioritizers());
		}
	}

	public InputConnectionYML(ConnectionEntity connection, String receiver) {
		this(connection);

		this.toPort = receiver;
	}
}
