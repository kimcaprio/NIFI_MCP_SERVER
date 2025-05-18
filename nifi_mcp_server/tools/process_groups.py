from typing import Dict, Any, List, Optional
from ..nifi_api import NiFiAPIClient
from loguru import logger

# Process Group Tools

async def list_process_groups(nifi_client: NiFiAPIClient, parent_id: str = "root") -> Dict[str, Any]:
    """List process groups in a parent group.
    
    Args:
        nifi_client: NiFi API client instance
        parent_id: ID of the parent process group (default: root)
        
    Returns:
        Dictionary with process group information
    """
    try:
        response = nifi_client.get(f"/flow/process-groups/{parent_id}")
        process_groups = response.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
        
        # Extract relevant information
        result = [
            {
                "id": pg.get("id"),
                "name": pg.get("name"),
                "comments": pg.get("comments"),
                "running_count": pg.get("runningCount"),
                "stopped_count": pg.get("stoppedCount"),
                "invalid_count": pg.get("invalidCount"),
                "disabled_count": pg.get("disabledCount"),
            }
            for pg in process_groups
        ]
        
        return {
            "status": "success",
            "parent_id": parent_id,
            "process_groups": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error listing process groups: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_process_group_details(nifi_client: NiFiAPIClient, pg_id: str) -> Dict[str, Any]:
    """Get detailed information about a specific process group.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: Process group ID
        
    Returns:
        Dictionary with detailed process group information
    """
    try:
        response = nifi_client.get(f"/flow/process-groups/{pg_id}")
        pg_flow = response.get("processGroupFlow", {})
        
        # Get status information
        status_response = nifi_client.get(f"/flow/process-groups/{pg_id}/status")
        status = status_response.get("processGroupStatus", {})
        
        return {
            "status": "success",
            "id": pg_id,
            "name": pg_flow.get("breadcrumb", {}).get("breadcrumb", {}).get("name"),
            "comments": pg_flow.get("component", {}).get("comments"),
            "processors": len(pg_flow.get("flow", {}).get("processors", [])),
            "connections": len(pg_flow.get("flow", {}).get("connections", [])),
            "input_ports": len(pg_flow.get("flow", {}).get("inputPorts", [])),
            "output_ports": len(pg_flow.get("flow", {}).get("outputPorts", [])),
            "process_groups": len(pg_flow.get("flow", {}).get("processGroups", [])),
            "running_components": status.get("runningCount", 0),
            "stopped_components": status.get("stoppedCount", 0),
            "invalid_components": status.get("invalidCount", 0),
            "disabled_components": status.get("disabledCount", 0),
            "in_bytes": status.get("bytesIn", 0),
            "out_bytes": status.get("bytesOut", 0),
            "queued_bytes": status.get("bytesQueued", 0),
            "queued_count": status.get("flowFilesQueued", 0),
        }
    except Exception as e:
        logger.error(f"Error getting process group details: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def create_process_group(nifi_client: NiFiAPIClient, parent_id: str, name: str, 
                              position_x: int = 0, position_y: int = 0, comments: str = None) -> Dict[str, Any]:
    """Create a new process group.
    
    Args:
        nifi_client: NiFi API client instance
        parent_id: ID of the parent process group
        name: Name of the new process group
        position_x: X position (default: 0)
        position_y: Y position (default: 0)
        comments: Optional comments
        
    Returns:
        Dictionary with new process group information
    """
    try:
        # Prepare the request body
        request_body = {
            "component": {
                "name": name,
                "position": {
                    "x": position_x,
                    "y": position_y
                }
            },
            "revision": {
                "version": 0
            }
        }
        
        if comments:
            request_body["component"]["comments"] = comments
        
        # Make the API call
        response = nifi_client.post(f"/process-groups/{parent_id}/process-groups", request_body)
        
        return {
            "status": "success",
            "id": response.get("id"),
            "name": response.get("component", {}).get("name"),
            "uri": response.get("uri")
        }
    except Exception as e:
        logger.error(f"Error creating process group: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def delete_process_group(nifi_client: NiFiAPIClient, pg_id: str) -> Dict[str, Any]:
    """Delete a process group.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: Process group ID
        
    Returns:
        Dictionary with deletion status
    """
    try:
        # Get the current revision
        pg_info = nifi_client.get(f"/process-groups/{pg_id}")
        revision = pg_info.get("revision", {})
        
        # Make the API call with the correct revision
        nifi_client.delete(f"/process-groups/{pg_id}?version={revision.get('version', 0)}")
        
        return {
            "status": "success",
            "message": f"Process group {pg_id} deleted successfully"
        }
    except Exception as e:
        logger.error(f"Error deleting process group: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def update_process_group(nifi_client: NiFiAPIClient, pg_id: str, name: str = None, 
                             comments: str = None) -> Dict[str, Any]:
    """Update a process group's properties.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: Process group ID
        name: New name (optional)
        comments: New comments (optional)
        
    Returns:
        Dictionary with update status
    """
    try:
        # Get current process group info
        current_info = nifi_client.get(f"/process-groups/{pg_id}")
        component = current_info.get("component", {}).copy()
        
        # Update with new values if provided
        if name is not None:
            component["name"] = name
        
        if comments is not None:
            component["comments"] = comments
        
        # Prepare the request body
        request_body = {
            "component": component,
            "revision": current_info.get("revision")
        }
        
        # Make the API call
        response = nifi_client.put(f"/process-groups/{pg_id}", request_body)
        
        return {
            "status": "success",
            "id": pg_id,
            "name": response.get("component", {}).get("name"),
            "message": "Process group updated successfully"
        }
    except Exception as e:
        logger.error(f"Error updating process group: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_process_group_status(nifi_client: NiFiAPIClient, pg_id: str, 
                                 recursive: bool = True) -> Dict[str, Any]:
    """Get status information for a process group.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: Process group ID
        recursive: Whether to include status of all child components
        
    Returns:
        Dictionary with status information
    """
    try:
        # Build the URL with the recursive parameter
        url = f"/flow/process-groups/{pg_id}/status"
        if recursive:
            url += "?recursive=true"
        
        # Get the status information
        response = nifi_client.get(url)
        status = response.get("processGroupStatus", {})
        
        # Extract and format the relevant information
        result = {
            "id": status.get("id"),
            "name": status.get("name"),
            "stats": {
                "read": status.get("bytesRead", 0),
                "written": status.get("bytesWritten", 0),
                "input": status.get("input", "0"),
                "output": status.get("output", "0"),
                "queued": status.get("queued", "0"),
                "queued_count": status.get("flowFilesQueued", 0),
                "queued_bytes": status.get("bytesQueued", 0),
            },
            "component_counts": {
                "running": status.get("runningCount", 0),
                "stopped": status.get("stoppedCount", 0),
                "invalid": status.get("invalidCount", 0),
                "disabled": status.get("disabledCount", 0),
                "total": status.get("runningCount", 0) + status.get("stoppedCount", 0) + 
                        status.get("invalidCount", 0) + status.get("disabledCount", 0)
            }
        }
        
        return {
            "status": "success",
            "recursive": recursive,
            "process_group_status": result
        }
    except Exception as e:
        logger.error(f"Error getting process group status: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Additional process group operations will be added here
