from typing import Dict, Any, List, Optional
from ..nifi_api import NiFiAPIClient
from loguru import logger

# Flow Control Tools

async def get_flow_status(nifi_client: NiFiAPIClient, pg_id: str = "root") -> Dict[str, Any]:
    """Get the status of a flow or process group.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: ID of the process group (default: root)
        
    Returns:
        Dictionary with flow status information
    """
    try:
        # Get the process group status
        response = nifi_client.get(f"/flow/process-groups/{pg_id}/status")
        status = response.get("processGroupStatus", {})
        
        # Get cluster information
        cluster_info = {}
        try:
            cluster_response = nifi_client.get("/controller/cluster")
            cluster_info = {
                "connected_nodes": len(cluster_response.get("cluster", {}).get("nodes", [])),
                "cluster_coordinator": any(node.get("roles", {}).get("isCoordinator", False) 
                                        for node in cluster_response.get("cluster", {}).get("nodes", []))
            }
        except Exception as e:
            logger.debug(f"Could not get cluster info, might be standalone: {str(e)}")
            cluster_info = {"connected_nodes": 1, "cluster_coordinator": True}
        
        return {
            "status": "success",
            "id": pg_id,
            "name": status.get("name"),
            "component_status": {
                "running": status.get("runningCount", 0),
                "stopped": status.get("stoppedCount", 0),
                "invalid": status.get("invalidCount", 0),
                "disabled": status.get("disabledCount", 0),
                "total": (status.get("runningCount", 0) + status.get("stoppedCount", 0) + 
                         status.get("invalidCount", 0) + status.get("disabledCount", 0))
            },
            "flow_status": {
                "bytes_in": status.get("bytesIn", 0),
                "bytes_out": status.get("bytesOut", 0),
                "bytes_queued": status.get("bytesQueued", 0),
                "flowfiles_queued": status.get("flowFilesQueued", 0),
                "input": status.get("input", "0"),
                "output": status.get("output", "0"),
                "queued": status.get("queued", "0")
            },
            "cluster": cluster_info
        }
    except Exception as e:
        logger.error(f"Error getting flow status: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def start_component(nifi_client: NiFiAPIClient, component_id: str, component_type: str) -> Dict[str, Any]:
    """Start a component (processor, process group, port, etc.).
    
    Args:
        nifi_client: NiFi API client instance
        component_id: ID of the component to start
        component_type: Type of component (processor, process-group, port, etc.)
        
    Returns:
        Dictionary with operation status
    """
    try:
        # Route based on component type
        if component_type == "processor":
            # Get current processor info
            current_info = nifi_client.get(f"/processors/{component_id}")
            
            # Prepare the request body
            request_body = {
                "component": {
                    "id": component_id,
                    "state": "RUNNING"
                },
                "revision": current_info.get("revision")
            }
            
            # Make the API call
            response = nifi_client.put(f"/processors/{component_id}", request_body)
            
            return {
                "status": "success",
                "id": component_id,
                "type": "processor",
                "name": response.get("component", {}).get("name"),
                "state": response.get("component", {}).get("state"),
                "message": "Processor started successfully"
            }
            
        elif component_type == "process-group":
            # Start all components in process group
            nifi_client.put(f"/flow/process-groups/{component_id}", {
                "id": component_id,
                "state": "RUNNING",
                "disconnectedNodeAcknowledged": False
            })
            
            return {
                "status": "success",
                "id": component_id,
                "type": "process-group",
                "message": "Process group started successfully"
            }
            
        elif component_type in ["input-port", "output-port"]:
            # Determine the endpoint
            endpoint = "/input-ports/" if component_type == "input-port" else "/output-ports/"
            
            # Get current port info
            current_info = nifi_client.get(f"{endpoint}{component_id}")
            
            # Prepare the request body
            request_body = {
                "component": {
                    "id": component_id,
                    "state": "RUNNING"
                },
                "revision": current_info.get("revision")
            }
            
            # Make the API call
            response = nifi_client.put(f"{endpoint}{component_id}", request_body)
            
            return {
                "status": "success",
                "id": component_id,
                "type": component_type,
                "name": response.get("component", {}).get("name"),
                "state": response.get("component", {}).get("state"),
                "message": f"{component_type.replace('-', ' ').title()} started successfully"
            }
            
        else:
            return {
                "status": "error",
                "message": f"Unsupported component type: {component_type}"
            }
    except Exception as e:
        logger.error(f"Error starting component: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def stop_component(nifi_client: NiFiAPIClient, component_id: str, component_type: str) -> Dict[str, Any]:
    """Stop a component (processor, process group, port, etc.).
    
    Args:
        nifi_client: NiFi API client instance
        component_id: ID of the component to stop
        component_type: Type of component (processor, process-group, port, etc.)
        
    Returns:
        Dictionary with operation status
    """
    try:
        # Route based on component type
        if component_type == "processor":
            # Get current processor info
            current_info = nifi_client.get(f"/processors/{component_id}")
            
            # Prepare the request body
            request_body = {
                "component": {
                    "id": component_id,
                    "state": "STOPPED"
                },
                "revision": current_info.get("revision")
            }
            
            # Make the API call
            response = nifi_client.put(f"/processors/{component_id}", request_body)
            
            return {
                "status": "success",
                "id": component_id,
                "type": "processor",
                "name": response.get("component", {}).get("name"),
                "state": response.get("component", {}).get("state"),
                "message": "Processor stopped successfully"
            }
            
        elif component_type == "process-group":
            # Stop all components in process group
            nifi_client.put(f"/flow/process-groups/{component_id}", {
                "id": component_id,
                "state": "STOPPED",
                "disconnectedNodeAcknowledged": False
            })
            
            return {
                "status": "success",
                "id": component_id,
                "type": "process-group",
                "message": "Process group stopped successfully"
            }
            
        elif component_type in ["input-port", "output-port"]:
            # Determine the endpoint
            endpoint = "/input-ports/" if component_type == "input-port" else "/output-ports/"
            
            # Get current port info
            current_info = nifi_client.get(f"{endpoint}{component_id}")
            
            # Prepare the request body
            request_body = {
                "component": {
                    "id": component_id,
                    "state": "STOPPED"
                },
                "revision": current_info.get("revision")
            }
            
            # Make the API call
            response = nifi_client.put(f"{endpoint}{component_id}", request_body)
            
            return {
                "status": "success",
                "id": component_id,
                "type": component_type,
                "name": response.get("component", {}).get("name"),
                "state": response.get("component", {}).get("state"),
                "message": f"{component_type.replace('-', ' ').title()} stopped successfully"
            }
            
        else:
            return {
                "status": "error",
                "message": f"Unsupported component type: {component_type}"
            }
    except Exception as e:
        logger.error(f"Error stopping component: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_component_status(nifi_client: NiFiAPIClient, component_id: str, component_type: str) -> Dict[str, Any]:
    """Get the status of a specific component.
    
    Args:
        nifi_client: NiFi API client instance
        component_id: ID of the component
        component_type: Type of component (processor, process-group, etc.)
        
    Returns:
        Dictionary with component status
    """
    try:
        # Route based on component type
        if component_type == "processor":
            response = nifi_client.get(f"/processors/{component_id}/status")
            status = response.get("processorStatus", {})
            
            return {
                "status": "success",
                "id": component_id,
                "type": "processor",
                "name": status.get("name"),
                "state": status.get("runStatus"),
                "stats": {
                    "input": status.get("input", "0"),
                    "output": status.get("output", "0"),
                    "bytes_read": status.get("bytesRead", 0),
                    "bytes_written": status.get("bytesWritten", 0),
                    "tasks_completed": status.get("taskCount", 0),
                    "tasks_duration_ns": status.get("taskNanoseconds", 0),
                    "active_threads": status.get("activeThreadCount", 0)
                }
            }
            
        elif component_type == "process-group":
            response = nifi_client.get(f"/flow/process-groups/{component_id}/status")
            status = response.get("processGroupStatus", {})
            
            return {
                "status": "success",
                "id": component_id,
                "type": "process-group",
                "name": status.get("name"),
                "components": {
                    "running": status.get("runningCount", 0),
                    "stopped": status.get("stoppedCount", 0),
                    "invalid": status.get("invalidCount", 0),
                    "disabled": status.get("disabledCount", 0)
                },
                "stats": {
                    "input": status.get("input", "0"),
                    "output": status.get("output", "0"),
                    "queued": status.get("queued", "0"),
                    "flowfiles_queued": status.get("flowFilesQueued", 0),
                    "bytes_in": status.get("bytesIn", 0),
                    "bytes_out": status.get("bytesOut", 0),
                    "bytes_queued": status.get("bytesQueued", 0)
                }
            }
            
        elif component_type == "connection":
            response = nifi_client.get(f"/connections/{component_id}/status")
            status = response.get("connectionStatus", {})
            
            return {
                "status": "success",
                "id": component_id,
                "type": "connection",
                "name": status.get("name"),
                "queue_stats": {
                    "flowfiles_count": status.get("flowFilesCount", 0),
                    "bytes_queued": status.get("bytesQueued", 0),
                    "queued": status.get("queued", "0"),
                    "input": status.get("input", "0"),
                    "output": status.get("output", "0")
                }
            }
            
        else:
            return {
                "status": "error",
                "message": f"Unsupported component type for status: {component_type}"
            }
    except Exception as e:
        logger.error(f"Error getting component status: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        } 