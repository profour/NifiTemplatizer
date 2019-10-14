package dev.nifi.yml;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.nifi.api.toolkit.model.ConnectableDTO.TypeEnum;
import org.apache.nifi.api.toolkit.model.ConnectionDTO;
import org.apache.nifi.api.toolkit.model.ConnectionEntity;
import org.apache.nifi.api.toolkit.model.ConnectionEntity.SourceTypeEnum;
import org.apache.nifi.api.toolkit.model.PositionDTO;

public class InputConnectionYML {

	/**
	 * Source processor flowfiles are coming from
	 */
	public String source;

	/**
	 * Relationships connecting from the source processor or
	 *  an output port from the source processor
	 */
	public final List<String> from = new ArrayList<>();

	/**
	 * Destination port the input link is connecting to on the ProcessGroup
	 */
	public String to;

	/**
	 * Special properties for the incoming connection (queuing type, queue size,
	 * etc)
	 */
	public final Map<String, Object> properties = new TreeMap<>();;

	/**
	 * Aesthetic positioning of bends in the connection link between processors
	 */
	public final List<String> position = new ArrayList<String>();
	
	/**
	 * Only for Jackson deserialization
	 */
	public InputConnectionYML() {}

	public InputConnectionYML(ConnectionEntity connection) {

		// If the source is a REMOTE_OUTPUT_PORT, we should reference the (remote) group
		// id, rather than the regular id
		this.source = connection.getSourceType() == SourceTypeEnum.REMOTE_OUTPUT_PORT ||
				      connection.getSourceType() == SourceTypeEnum.OUTPUT_PORT 
				      ? connection.getSourceGroupId() : connection.getSourceId();

		ConnectionDTO conn = connection.getComponent();

		// If the incoming connection is from a processor, check to see the relationships that 
		// flowfiles are coming from
		if (conn.getSelectedRelationships() != null) {
			this.from.addAll(conn.getSelectedRelationships());
		}

		// If our input is coming from another process group, 
		// we need to identify the port name
		if (conn.getSource().getType() == TypeEnum.OUTPUT_PORT
				|| conn.getSource().getType() == TypeEnum.REMOTE_OUTPUT_PORT) {
			this.from.add(conn.getSource().getName());
		}
		
		// If this is connecting to a process group, need to map the input onto
		// the correct input port inside the process group
		if (conn.getDestination().getType() == TypeEnum.INPUT_PORT
				|| conn.getDestination().getType() == TypeEnum.REMOTE_INPUT_PORT) {
			this.to = conn.getDestination().getName();
		}

		// See if any manual position of the link was performed (aesthetic only)
		if (!conn.getBends().isEmpty()) {
			for (PositionDTO pos : conn.getBends()) {
				// Truncate positioning of bends to be integer values only
				this.position.add(HelperYML.formatPosition(pos.getX(), pos.getY()));
			}
		}

		// Try to determine if any link properties have changed from their defaults
		if (conn.getName() != null && !conn.getName().isEmpty()) {
			this.properties.put(HelperYML.NAME, conn.getName());
		}
		if (conn.getGetzIndex() != null) {
			this.properties.put(HelperYML.Z_INDEX, conn.getGetzIndex());
		}
		if (conn.getLabelIndex() != null && conn.getLabelIndex() != 1) {
			this.properties.put(HelperYML.LABEL_INDEX, conn.getLabelIndex());
		}
		if (conn.getBackPressureObjectThreshold() != HelperYML.DEFAULT_BACK_PRESSURE_OBJECT_THRESHOLD) {
			this.properties.put(HelperYML.BACK_PRESSURE_OBJECT_THRESHOLD, conn.getBackPressureObjectThreshold().toString());
		}
		if (!conn.getBackPressureDataSizeThreshold().equals(HelperYML.DEFAULT_BACK_PRESSURE_DATA_SIZE_THRESHOLD)) {
			this.properties.put(HelperYML.BACK_PRESSURE_DATA_SIZE_THRESHOLD, conn.getBackPressureDataSizeThreshold());
		}
		if (conn.getLoadBalanceStrategy() != HelperYML.DEFAULT_LOAD_BALANCE_STRATEGY) {
			this.properties.put(HelperYML.LOAD_BALANCE_STRATEGY, conn.getLoadBalanceStrategy().name());
		}
		if (conn.getLoadBalancePartitionAttribute() != null && !conn.getLoadBalancePartitionAttribute().isEmpty()) {
			this.properties.put(HelperYML.LOAD_BALANCE_PARTITION_ATTRIBUTE, conn.getLoadBalancePartitionAttribute());
		}
		if (conn.getLoadBalanceCompression() != HelperYML.DEFAULT_LOAD_BALANCE_COMPRESSION) {
			this.properties.put(HelperYML.LOAD_BALANCE_COMPRESSION, conn.getLoadBalanceCompression().name());
		}
		if (!conn.getFlowFileExpiration().equals(HelperYML.DEFAULT_FLOW_FILE_EXPIRATION_SEC) &&
			!conn.getFlowFileExpiration().equals(HelperYML.DEFAULT_FLOW_FILE_EXPIRATION_MINS)) {
			this.properties.put(HelperYML.FLOW_FILE_EXPIRATION, conn.getFlowFileExpiration());
		}
		if (!conn.getPrioritizers().isEmpty()) {
			this.properties.put(HelperYML.PRIORITIZERS, conn.getPrioritizers());
		}
	}
}
