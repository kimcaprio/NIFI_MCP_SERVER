import json
import requests
from typing import Dict, Any, Optional, Union
from loguru import logger

class NiFiAPIClient:
    """Client for interacting with Apache NiFi API."""
    
    def __init__(self, base_url: str, auth_type: str = "none", 
                 username: str = None, password: str = None, 
                 token: str = None, ssl_verify: bool = True):
        """Initialize the NiFi API client.
        
        Args:
            base_url: Base URL for the NiFi API (e.g., http://localhost:8080/nifi-api)
            auth_type: Type of authentication (none, basic, token)
            username: Username for basic authentication
            password: Password for basic authentication
            token: Access token for token authentication
            ssl_verify: Whether to verify SSL certificates
        """
        self.base_url = base_url.rstrip("/")
        self.auth_type = auth_type.lower()
        self.username = username
        self.password = password
        self.token = token
        self.ssl_verify = ssl_verify
        
        # Basic validation
        if self.auth_type == "basic" and (not username or not password):
            logger.warning("Basic authentication selected but username or password is missing")
            
        if self.auth_type == "token" and not token:
            logger.warning("Token authentication selected but token is missing")
            
        logger.info(f"Initialized NiFi API client for {self.base_url} with {self.auth_type} authentication")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get headers for API requests with authentication.
        
        Returns:
            Dictionary of HTTP headers
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        
        if self.auth_type == "token":
            headers["Authorization"] = f"Bearer {self.token}"
            
        return headers
    
    def _get_auth(self) -> Optional[tuple]:
        """Get authentication for requests.
        
        Returns:
            Tuple for basic auth or None
        """
        if self.auth_type == "basic":
            return (self.username, self.password)
        return None
    
    def _make_request(self, method: str, endpoint: str, data: Dict[str, Any] = None,
                     params: Dict[str, Any] = None, headers: Dict[str, str] = None) -> Any:
        """Make an HTTP request to the NiFi API.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint (without base URL)
            data: Request body data
            params: URL parameters
            headers: Additional headers
            
        Returns:
            Response data as JSON or string
        """
        url = f"{self.base_url}{endpoint}"
        request_headers = self._get_headers()
        
        # Merge additional headers if provided
        if headers:
            request_headers.update(headers)
            
        auth = self._get_auth()
        
        try:
            response = requests.request(
                method=method,
                url=url,
                json=data,
                params=params,
                headers=request_headers,
                auth=auth,
                verify=self.ssl_verify
            )
            
            # Raise exception for HTTP errors
            response.raise_for_status()
            
            # Return data as JSON if possible, otherwise as text
            if "application/json" in response.headers.get("Content-Type", ""):
                return response.json()
            return response.text
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            # Try to get more details from the response
            try:
                error_detail = response.json()
                logger.error(f"API error details: {json.dumps(error_detail)}")
            except:
                logger.error(f"Response text: {response.text}")
            raise
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            raise
    
    def get(self, endpoint: str, params: Dict[str, Any] = None) -> Any:
        """Make a GET request to the NiFi API.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: URL parameters
            
        Returns:
            Response data
        """
        return self._make_request("GET", endpoint, params=params)
    
    def post(self, endpoint: str, data: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Any:
        """Make a POST request to the NiFi API.
        
        Args:
            endpoint: API endpoint (without base URL)
            data: Request body data
            params: URL parameters
            
        Returns:
            Response data
        """
        return self._make_request("POST", endpoint, data=data, params=params)
    
    def put(self, endpoint: str, data: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Any:
        """Make a PUT request to the NiFi API.
        
        Args:
            endpoint: API endpoint (without base URL)
            data: Request body data
            params: URL parameters
            
        Returns:
            Response data
        """
        return self._make_request("PUT", endpoint, data=data, params=params)
    
    def delete(self, endpoint: str, params: Dict[str, Any] = None) -> Any:
        """Make a DELETE request to the NiFi API.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: URL parameters
            
        Returns:
            Response data or None
        """
        return self._make_request("DELETE", endpoint, params=params)
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the connection to the NiFi API.
        
        Returns:
            Dictionary with connection status information
        """
        try:
            # Try to get the NiFi root information
            response = self.get("/flow/about")
            
            return {
                "status": "success",
                "connected": True,
                "nifi_version": response.get("about", {}).get("version", "unknown"),
                "build_tag": response.get("about", {}).get("buildTag", "unknown"),
                "build_revision": response.get("about", {}).get("buildRevision", "unknown"),
                "build_timestamp": response.get("about", {}).get("buildTimestamp", "unknown"),
                "nifi_timezone": response.get("about", {}).get("timezone", "unknown"),
                "authenticated": self.auth_type != "none"
            }
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return {
                "status": "error",
                "connected": False,
                "message": str(e)
            }
