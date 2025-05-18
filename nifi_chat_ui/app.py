import streamlit as st
import httpx
import json
import os
from typing import Dict, Any, List
import uuid

# Set page title and configuration
st.set_page_config(
    page_title="NiFi Chat Assistant",
    page_icon="🔄",
    layout="wide"
)

# Initialize session state variables
if "messages" not in st.session_state:
    st.session_state.messages = []

if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

# App configuration
MCP_SERVER_URL = os.environ.get("MCP_SERVER_URL", "http://localhost:8000")
API_ENDPOINT = f"{MCP_SERVER_URL}/api/chat"

# Custom styling
st.markdown("""
<style>
.user-message {
    background-color: #e6f7ff;
    padding: 10px;
    border-radius: 10px;
    margin-bottom: 10px;
}
.assistant-message {
    background-color: #f0f0f0;
    padding: 10px;
    border-radius: 10px;
    margin-bottom: 10px;
}
.small-font {
    font-size: 12px;
    color: #888;
}
</style>
""", unsafe_allow_html=True)

# App title
st.title("NiFi Chat Assistant")
st.caption("Ask natural language questions about your Apache NiFi instance")

# Sidebar with configuration
with st.sidebar:
    st.header("Configuration")
    
    # Server configuration
    server_url = st.text_input("MCP Server URL", value=MCP_SERVER_URL)
    
    # NiFi connection information
    st.subheader("NiFi Connection")
    nifi_url = st.text_input("NiFi URL", "http://localhost:8080/nifi-api")
    
    # Authentication settings
    st.subheader("Authentication")
    auth_type = st.selectbox("Authentication Type", ["None", "Basic", "Token"])
    
    if auth_type == "Basic":
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
    elif auth_type == "Token":
        token = st.text_input("Token", type="password")
    
    # Advanced settings
    with st.expander("Advanced Settings"):
        verify_ssl = st.checkbox("Verify SSL", value=True)
        debug_mode = st.checkbox("Debug Mode", value=False)
    
    # Connection test button
    if st.button("Test Connection"):
        with st.spinner("Testing connection..."):
            try:
                response = httpx.get(f"{server_url}")
                if response.status_code == 200:
                    st.success("Successfully connected to MCP server!")
                else:
                    st.error(f"Failed to connect to MCP server: {response.status_code}")
            except Exception as e:
                st.error(f"Error connecting to MCP server: {str(e)}")
    
    # Clear chat button
    if st.button("Clear Chat"):
        st.session_state.messages = []
        st.session_state.session_id = str(uuid.uuid4())
        st.success("Chat history cleared!")

# Display chat history
for message in st.session_state.messages:
    if message["role"] == "user":
        st.markdown(f"<div class='user-message'><strong>You:</strong> {message['content']}</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div class='assistant-message'><strong>Assistant:</strong> {message['content']}</div>", unsafe_allow_html=True)
        if "action_taken" in message:
            st.markdown(f"<div class='small-font'>Action: {message['action_taken']}</div>", unsafe_allow_html=True)

# Input for new messages
user_input = st.chat_input("Ask about your NiFi instance...")

if user_input:
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": user_input})
    
    # Display user message (this will be updated when the page refreshes)
    st.markdown(f"<div class='user-message'><strong>You:</strong> {user_input}</div>", unsafe_allow_html=True)
    
    # Send request to MCP server
    with st.spinner("Thinking..."):
        try:
            payload = {
                "query": user_input,
                "session_id": st.session_state.session_id,
                "user_id": "streamlit_user",
                "context": {
                    "nifi_url": nifi_url,
                    "auth_type": auth_type.lower() if auth_type != "None" else "none"
                }
            }
            
            if auth_type == "Basic":
                payload["context"]["username"] = username
                payload["context"]["password"] = password
            elif auth_type == "Token":
                payload["context"]["token"] = token
            
            response = httpx.post(
                API_ENDPOINT,
                json=payload,
                timeout=30.0
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Add assistant message to chat history
                assistant_message = {
                    "role": "assistant",
                    "content": result.get("response", "I encountered an error processing your request.")
                }
                
                # Include action information if available
                if result.get("action_taken"):
                    assistant_message["action_taken"] = result.get("action_taken")
                
                st.session_state.messages.append(assistant_message)
                
                # Display assistant message
                st.markdown(f"<div class='assistant-message'><strong>Assistant:</strong> {assistant_message['content']}</div>", unsafe_allow_html=True)
                
                if debug_mode and "context_updates" in result:
                    with st.expander("Debug Information"):
                        st.json(result.get("context_updates", {}))
            else:
                error_msg = f"Error from server: {response.status_code}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": f"I'm sorry, I encountered an error: {error_msg}"
                })
        except Exception as e:
            error_msg = f"Failed to communicate with the MCP server: {str(e)}"
            st.error(error_msg)
            st.session_state.messages.append({
                "role": "assistant", 
                "content": f"I'm sorry, I encountered an error: {error_msg}"
            })

# Footer
st.markdown("---")
st.markdown("<div class='small-font'>NiFi Chat Assistant | Built with Streamlit and FastAPI</div>", unsafe_allow_html=True)
