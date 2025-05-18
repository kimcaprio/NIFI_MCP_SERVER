from typing import Dict, Any, List, Optional
from ..nifi_api import NiFiAPIClient
from loguru import logger

# Connection Tools

async def list_connections(nifi_client: NiFiAPIClient, pg_id: str = "root") -> Dict[str, Any]:
    """List connections in a process group.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: ID of the parent process group (default: root)
        
    Returns:
        Dictionary with connection information
    """
    try:
        response = nifi_client.get(f"/flow/process-groups/{pg_id}")
        connections = response.get("processGroupFlow", {}).get("flow", {}).get("connections", [])
        
        # Extract relevant information
        result = [
            {
                "id": conn.get("id"),
                "name": conn.get("name"),
                "source": {
                    "id": conn.get("sourceId"),
                    "name": conn.get("sourceConnectable", {}).get("name"),
                    "type": conn.get("sourceType"),
                    "group_name": conn.get("sourceGroupName")
                },
                "destination": {
                    "id": conn.get("destinationId"),
                    "name": conn.get("destinationConnectable", {}).get("name"),
                    "type": conn.get("destinationType"),
                    "group_name": conn.get("destinationGroupName")
                },
                "selected_relationships": conn.get("selectedRelationships", []),
                "flow_files_count": conn.get("status", {}).get("flowFilesCount", 0),
                "queued_size": conn.get("status", {}).get("queued", "0 B")
            }
            for conn in connections
        ]
        
        return {
            "status": "success",
            "parent_id": pg_id,
            "connections": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error listing connections: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_connection_details(nifi_client: NiFiAPIClient, connection_id: str) -> Dict[str, Any]:
    """Get detailed information about a specific connection.
    
    Args:
        nifi_client: NiFi API client instance
        connection_id: Connection ID
        
    Returns:
        Dictionary with detailed connection information
    """
    try:
        response = nifi_client.get(f"/connections/{connection_id}")
        component = response.get("component", {})
        
        # Get status information
        status_response = nifi_client.get(f"/connections/{connection_id}/status")
        status = status_response.get("connectionStatus", {})
        
        return {
            "status": "success",
            "id": connection_id,
            "name": component.get("name"),
            "source": {
                "id": component.get("source", {}).get("id"),
                "name": component.get("source", {}).get("name"),
                "type": component.get("source", {}).get("type"),
                "group_id": component.get("source", {}).get("groupId"),
                "group_name": component.get("source", {}).get("groupName")
            },
            "destination": {
                "id": component.get("destination", {}).get("id"),
                "name": component.get("destination", {}).get("name"),
                "type": component.get("destination", {}).get("type"),
                "group_id": component.get("destination", {}).get("groupId"),
                "group_name": component.get("destination", {}).get("groupName")
            },
            "selected_relationships": component.get("selectedRelationships", []),
            "flow_file_expiration": component.get("flowFileExpiration"),
            "backpressure": {
                "object_threshold": component.get("backPressureObjectThreshold"),
                "data_size_threshold": component.get("backPressureDataSizeThreshold")
            },
            "prioritizers": component.get("prioritizers", []),
            "bends": component.get("bends", []),
            "load_balance_strategy": component.get("loadBalanceStrategy"),
            "load_balance_partition_attribute": component.get("loadBalancePartitionAttribute"),
            "load_balance_compression": component.get("loadBalanceCompression"),
            "queue_status": {
                "flow_files_count": status.get("aggregateSnapshot", {}).get("flowFilesCount", 0),
                "queued_size": status.get("aggregateSnapshot", {}).get("bytesQueued", 0),
                "queued_size_formatted": status.get("aggregateSnapshot", {}).get("queued", "0 B"),
                "input_count": status.get("aggregateSnapshot", {}).get("input", "0"),
                "output_count": status.get("aggregateSnapshot", {}).get("output", "0")
            }
        }
    except Exception as e:
        logger.error(f"Error getting connection details: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def create_connection(nifi_client: NiFiAPIClient, pg_id: str, source_id: str, destination_id: str,
                          source_type: str, destination_type: str,
                          selected_relationships: List[str] = None,
                          name: str = None) -> Dict[str, Any]:
    """Create a new connection between components.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: Process group ID where the connection will be created
        source_id: ID of the source component
        destination_id: ID of the destination component
        source_type: Type of the source component (PROCESSOR, FUNNEL, INPUT_PORT, OUTPUT_PORT, REMOTE_INPUT_PORT)
        destination_type: Type of the destination component (PROCESSOR, FUNNEL, INPUT_PORT, OUTPUT_PORT, REMOTE_OUTPUT_PORT)
        selected_relationships: List of relationships to include in the connection
        name: Optional name for the connection
        
    Returns:
        Dictionary with new connection information
    """
    try:
        # Prepare the request body
        request_body = {
            "component": {
                "source": {
                    "id": source_id,
                    "type": source_type
                },
                "destination": {
                    "id": destination_id,
                    "type": destination_type
                },
                "selectedRelationships": selected_relationships or []
            },
            "revision": {
                "version": 0
            }
        }
        
        if name:
            request_body["component"]["name"] = name
        
        # Make the API call
        response = nifi_client.post(f"/process-groups/{pg_id}/connections", request_body)
        
        return {
            "status": "success",
            "id": response.get("id"),
            "name": response.get("component", {}).get("name"),
            "source_id": source_id,
            "destination_id": destination_id,
            "uri": response.get("uri")
        }
    except Exception as e:
        logger.error(f"Error creating connection: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def update_connection(nifi_client: NiFiAPIClient, connection_id: str, 
                          selected_relationships: List[str] = None,
                          flow_file_expiration: str = None,
                          backpressure_object_threshold: int = None,
                          backpressure_data_size_threshold: str = None,
                          prioritizers: List[str] = None,
                          load_balance_strategy: str = None) -> Dict[str, Any]:
    """Update a connection's configuration.
    
    Args:
        nifi_client: NiFi API client instance
        connection_id: ID of the connection to update
        selected_relationships: List of relationships to include
        flow_file_expiration: FlowFile expiration time (e.g., "30 min")
        backpressure_object_threshold: Object count threshold for backpressure
        backpressure_data_size_threshold: Data size threshold for backpressure (e.g., "1 GB")
        prioritizers: List of prioritizer classes
        load_balance_strategy: Load balance strategy
        
    Returns:
        Dictionary with update status
    """
    try:
        # Get current connection info
        current_info = nifi_client.get(f"/connections/{connection_id}")
        component = current_info.get("component", {}).copy()
        
        # Update with new values if provided
        if selected_relationships is not None:
            component["selectedRelationships"] = selected_relationships
        
        if flow_file_expiration is not None:
            component["flowFileExpiration"] = flow_file_expiration
        
        if backpressure_object_threshold is not None:
            component["backPressureObjectThreshold"] = backpressure_object_threshold
        
        if backpressure_data_size_threshold is not None:
            component["backPressureDataSizeThreshold"] = backpressure_data_size_threshold
        
        if prioritizers is not None:
            component["prioritizers"] = prioritizers
        
        if load_balance_strategy is not None:
            component["loadBalanceStrategy"] = load_balance_strategy
        
        # Prepare the request body
        request_body = {
            "component": component,
            "revision": current_info.get("revision")
        }
        
        # Make the API call
        response = nifi_client.put(f"/connections/{connection_id}", request_body)
        
        return {
            "status": "success",
            "id": connection_id,
            "name": response.get("component", {}).get("name"),
            "message": "Connection updated successfully"
        }
    except Exception as e:
        logger.error(f"Error updating connection: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def delete_connection(nifi_client: NiFiAPIClient, connection_id: str) -> Dict[str, Any]:
    """Delete a connection.
    
    Args:
        nifi_client: NiFi API client instance
        connection_id: ID of the connection to delete
        
    Returns:
        Dictionary with deletion status
    """
    try:
        # Get current connection info to get the current revision
        current_info = nifi_client.get(f"/connections/{connection_id}")
        revision = current_info.get("revision", {})
        
        # Check if there are queued FlowFiles - can't delete a connection with queued data
        status = nifi_client.get(f"/connections/{connection_id}/status")
        if status.get("connectionStatus", {}).get("aggregateSnapshot", {}).get("flowFilesCount", 0) > 0:
            return {
                "status": "error",
                "message": "Cannot delete connection with queued FlowFiles. Empty the queue first."
            }
        
        # Make the API call with the correct revision
        nifi_client.delete(f"/connections/{connection_id}?version={revision.get('version', 0)}")
        
        return {
            "status": "success",
            "message": f"Connection {connection_id} deleted successfully"
        }
    except Exception as e:
        logger.error(f"Error deleting connection: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def empty_connection_queue(nifi_client: NiFiAPIClient, connection_id: str) -> Dict[str, Any]:
    """Empty a connection's queue.
    
    Args:
        nifi_client: NiFi API client instance
        connection_id: ID of the connection
        
    Returns:
        Dictionary with operation status
    """
    try:
        # Get current connection info to get the current revision
        current_info = nifi_client.get(f"/connections/{connection_id}")
        revision = current_info.get("revision", {})
        
        # Prepare the request body
        request_body = {
            "id": connection_id,
            "revision": revision
        }
        
        # Make the API call
        nifi_client.post(f"/connections/{connection_id}/drop-requests", request_body)
        
        return {
            "status": "success",
            "id": connection_id,
            "message": "Successfully initiated queue emptying operation"
        }
    except Exception as e:
        logger.error(f"Error emptying connection queue: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Additional connection operations can be added here
