from flask import Flask, render_template, request, jsonify
import requests
import json
import xml.etree.ElementTree as ET
import re
import traceback
import copy
import csv
import io
import os
from urllib.parse import urlparse, quote
from typing import List, Dict, Optional
from datetime import datetime, timedelta

# Create Flask application instance
app = Flask(__name__)

# Constants
API_VERSION = "3.24"

# ------------------------------
# Sign in helpers (from original CLI script)
# ------------------------------
def sign_in_rest(host, site_content_url, username=None, password=None, pat_name=None, pat_secret=None):
    """Sign in to Tableau Server using REST API"""
    url = f"{host}/api/{API_VERSION}/auth/signin"
    if pat_name and pat_secret:
        payload = {
            "credentials": {
                "personalAccessTokenName": pat_name, 
                "personalAccessTokenSecret": pat_secret, 
                "site": {"contentUrl": site_content_url}
            }
        }
    else:
        payload = {
            "credentials": {
                "name": username, 
                "password": password, 
                "site": {"contentUrl": site_content_url}
            }
        }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    r = requests.post(url, headers=headers, json=payload)
    r.raise_for_status()
    data = r.json()["credentials"]
    return data["token"], data["site"]["id"]

def force_sign_out(host, token=None):
    """Sign out from Tableau Server"""
    if token:
        url = f"{host}/api/{API_VERSION}/auth/signout"
        headers = {"X-Tableau-Auth": token}
        try:
            requests.post(url, headers=headers)
            return True
        except Exception:
            return False
    return False

# ------------------------------
# Datasource lookup
# ------------------------------
def get_datasource_id_rest(host, token, site_id, datasource_name):
    """Get datasource ID by name"""
    url = f"{host}/api/{API_VERSION}/sites/{site_id}/datasources"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    ds_list = r.json().get("datasources", {}).get("datasource", [])
    for ds in ds_list:
        if ds["name"] == datasource_name:
            return ds["id"]
    raise ValueError(f"Datasource '{datasource_name}' not found")

def get_all_datasources_rest(host, token, site_id, api_version):
    """Get all datasources on the site and return ID-to-name mapping"""
    url = f"{host}/api/{api_version}/sites/{site_id}/datasources?pageSize=1000"
    headers = {"X-Tableau-Auth": token, "Accept": "application/json"}
    
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        ds_list = r.json().get("datasources", {}).get("datasource", [])
        
        # Build ID to name mapping
        datasource_map = {}
        for ds in ds_list:
            ds_id = ds.get("id")
            ds_name = ds.get("name", "Unnamed Datasource")
            if ds_id:
                datasource_map[ds_id] = ds_name
        
        return {'success': True, 'datasources': datasource_map}
    except Exception as e:
        return {'success': False, 'error': f"Error getting datasources: {str(e)}"}

# ------------------------------
# Pulse API: definitions/metrics
# ------------------------------
def get_pulse_definition(host, definition_id, token):
    """Get pulse definition by ID"""
    url = f"{host}/api/-/pulse/definitions/{definition_id}"
    headers = {"X-Tableau-Auth": token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["definition"]

def create_pulse_definition(host, pulse_token, definition_payload):
    """Create new pulse definition"""
    url = f"{host}/api/-/pulse/definitions"
    headers = {"Content-Type": "application/json", "X-Tableau-Auth": pulse_token}
    r = requests.post(url, headers=headers, json=definition_payload)
    r.raise_for_status()
    return r.json()

# ------------------------------
# Build payload for destination site
# ------------------------------
def build_definition_payload(definition_a, datasource_id_b):
    """Build definition payload for destination site"""
    original_spec = definition_a.get("specification", {})
    spec = {}

    if "basic_specification" in original_spec:
        spec["basic_specification"] = original_spec["basic_specification"]
        spec["is_running_total"] = original_spec.get("is_running_total", False)
    elif "viz_state_specification" in original_spec:
        viz_spec = original_spec["viz_state_specification"].copy()
        if isinstance(viz_spec.get("viz_state_string"), dict):
            viz_spec["viz_state_string"] = json.dumps(viz_spec["viz_state_string"])
        spec["viz_state_specification"] = viz_spec
        spec["is_running_total"] = original_spec.get("is_running_total", False)
    else:
        raise ValueError("No recognizable specification in source definition")

    spec["datasource"] = {"id": datasource_id_b}

    comparisons = definition_a.get("comparisons", {}).get("comparisons", [])
    clean_comparisons = []
    for comp in comparisons:
        clean_comp = comp.copy()
        if "index" in clean_comp:
            clean_comp["index"] = int(clean_comp["index"])
        clean_comparisons.append(clean_comp)

    payload = {
        "name": definition_a["metadata"]["name"],
        "specification": spec,
        "extension_options": {
            "allowed_dimensions": original_spec.get("extension_options", {}).get("allowed_dimensions", []),
            "allowed_granularities": original_spec.get("extension_options", {}).get("allowed_granularities", []),
            "offset_from_today": original_spec.get("extension_options", {}).get("offset_from_today", 0),
            "correlation_candidate_definition_ids": original_spec.get("extension_options", {}).get("correlation_candidate_definition_ids", []),
            "use_dynamic_offset": original_spec.get("extension_options", {}).get("use_dynamic_offset", False),
        },
        "representation_options": original_spec.get("representation_options", {"type": "NUMBER_FORMAT_TYPE_NUMBER", "sentiment_type": "SENTIMENT_TYPE_NONE"}),
        "insights_options": original_spec.get("insights_options", {"show_insights": True, "settings":[]}),
        "comparisons": {"comparisons": clean_comparisons},
        "datasource_goals": definition_a.get("datasource_goals", []),
        "related_links": definition_a.get("related_links", []),
        "certification": {"is_certified": False}
    }

    return payload

# ------------------------------
# Definition selection helper
# ------------------------------
def get_definitions_to_copy(host, token, datasource_id, choice):
    """Get definition IDs to copy based on choice"""
    if choice.lower() == "all":
        url = f"{host}/api/-/pulse/definitions"
        headers = {"X-Tableau-Auth": token}
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        all_defs = r.json().get("definitions", [])
        defs_for_ds = [
            d.get("metadata", {}).get("id")
            for d in all_defs
            if d.get("specification", {}).get("datasource", {}).get("id") == datasource_id
               and d.get("metadata", {}).get("id")  # only include if ID exists
        ]
        return defs_for_ds
    else:
        return [d.strip() for d in choice.split(",") if d.strip()]

# ------------------------------
# Bulk Manage Followers Functions
# ------------------------------

def sign_in_rest_xml(server, site, auth_type, username=None, password=None, pat_name=None, pat_token=None):
    """XML-based sign in for bulk followers functionality"""
    url = f"{server}/api/{API_VERSION}/auth/signin"
    headers = {"Content-Type": "application/xml"}
    
    if auth_type == "password":
        xml_payload = f"""
        <tsRequest>
            <credentials name="{username}" password="{password}">
                <site contentUrl="{site}" />
            </credentials>
        </tsRequest>
        """
    elif auth_type == "pat":
        xml_payload = f"""
        <tsRequest>
            <credentials personalAccessTokenName="{pat_name}" personalAccessTokenSecret="{pat_token}">
                <site contentUrl="{site}" />
            </credentials>
        </tsRequest>
        """
    else:
        raise ValueError("Unknown auth_type")

    r = requests.post(url, data=xml_payload.encode("utf-8"), headers=headers)
    r.raise_for_status()

    # Parse XML to get token and site_id
    root = ET.fromstring(r.text)
    token = root.find(".//{http://tableau.com/api}credentials").attrib["token"]
    site_id = root.find(".//{http://tableau.com/api}site").attrib["id"]
    return token, site_id

def get_user_id_by_email(server, token, site_id, email):
    """Get user ID by email address"""
    url = f"{server}/api/{API_VERSION}/sites/{site_id}/users"
    headers = {"X-Tableau-Auth": token}

    r = requests.get(url, headers=headers)
    r.raise_for_status()

    root = ET.fromstring(r.text)
    users = root.findall(".//{http://tableau.com/api}user")
    for user in users:
        if user.attrib["name"].lower() == email.lower():
            return user.attrib["id"]
    raise ValueError(f"User {email} not found on site.")

def get_metric_followers(pulse_server, pulse_token, metric_id):
    """Get existing followers for a metric"""
    url = f"{pulse_server}/api/-/pulse/subscriptions?metric_id={metric_id}&page_size=1000"
    headers = {"X-Tableau-Auth": pulse_token}

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()

    # Extract user IDs from subscriptions
    return [s["follower"]["user_id"] for s in data.get("subscriptions", [])]

def batch_create_subscriptions(pulse_server, pulse_token, metric_id, user_ids):
    """Add multiple followers to a metric using batchCreate endpoint"""
    if not user_ids:
        return {"success": True, "message": f"⚠ No new followers to add for metric {metric_id}"}
    
    # Use the exact payload format that works
    payload = {
        "metric_id": metric_id,
        "followers": [{"user_id": uid} for uid in user_ids]
    }
    
    url = f"{pulse_server}/api/-/pulse/subscriptions:batchCreate"
    headers = {"X-Tableau-Auth": pulse_token, "Content-Type": "application/json"}
    
    try:
        r = requests.post(url, headers=headers, json=payload)
        r.raise_for_status()
        return {"success": True, "message": f"✅ Added {len(user_ids)} followers to metric {metric_id}"}
    except requests.exceptions.HTTPError as e:
        # If batch create fails, provide detailed error info
        error_details = ""
        if hasattr(e, 'response') and e.response:
            try:
                error_details = f" - Response: {e.response.text}"
            except:
                error_details = f" - Status: {e.response.status_code}"
        
        return {"success": False, "message": f"❌ Failed to add followers to metric {metric_id}: {str(e)}{error_details}"}

def remove_followers(pulse_server, pulse_token, metric_id, user_ids_to_remove):
    """Remove users from a Pulse metric"""
    headers = {"X-Tableau-Auth": pulse_token}
    url = f"{pulse_server}/api/-/pulse/subscriptions?metric_id={metric_id}&page_size=1000"

    r = requests.get(url, headers=headers)
    r.raise_for_status()
    data = r.json()

    subscriptions = data.get("subscriptions", [])
    removed_count = 0

    for sub in subscriptions:
        sub_id = sub["id"]
        follower_id = sub["follower"]["user_id"]
        if follower_id in user_ids_to_remove:
            delete_url = f"{pulse_server}/api/-/pulse/subscriptions/{sub_id}"
            del_resp = requests.delete(delete_url, headers=headers)
            if del_resp.status_code == 204:
                removed_count += 1

    return {"success": True, "message": f"✅ Removed {removed_count} followers from metric {metric_id}"}

# ------------------------------
# Swap Datasources Functions  
# ------------------------------

def get_pulse_definition_for_swap(host, definition_id, token):
    """Get pulse definition for datasource swapping"""
    url = f"{host}/api/-/pulse/definitions/{definition_id}"
    headers = {"X-Tableau-Auth": token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()["definition"]

def create_pulse_definition_for_swap(host, token, definition_payload):
    """Create pulse definition for datasource swapping"""
    url = f"{host}/api/-/pulse/definitions"
    headers = {"X-Tableau-Auth": token, "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json=definition_payload)
    r.raise_for_status()
    return r.json()["definition"]

def get_metrics_for_definition_swap(host, definition_id, token):
    """Get metrics for a definition during datasource swap"""
    url = f"{host}/api/-/pulse/definitions/{definition_id}/metrics"
    headers = {"X-Tableau-Auth": token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json().get("metrics", [])

def create_metric_for_swap(host, definition_id, metric_payload, token):
    """Create metric during datasource swap"""
    url = f"{host}/api/-/pulse/metrics:getOrCreate"
    headers = {
        "X-Tableau-Auth": token,
        "Content-Type": "application/json"
    }

    payload = metric_payload.copy()
    payload["definition_id"] = definition_id

    r = requests.post(url, headers=headers, json=payload)
    r.raise_for_status()
    return r.json()

def get_subscriptions_for_swap(host, metric_id, token):
    """Get subscriptions for metric during datasource swap"""
    url = f"{host}/api/-/pulse/subscriptions?page_size=1000&metric_id={metric_id}"
    headers = {"X-Tableau-Auth": token, "Content-Type": "application/json"}    
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json().get("subscriptions", [])

def add_follower_for_swap(host, metric_id, user_id, token):
    """Add follower during datasource swap"""
    url = f"{host}/api/-/pulse/subscriptions"
    headers = {"X-Tableau-Auth": token, "Content-Type": "application/json"}    
    payload = {"metric_id": metric_id, "follower": {"user_id": user_id}}
    r = requests.post(url, headers=headers, json=payload)
    r.raise_for_status()
    return r.json()

def remove_subscription_for_swap(host, subscription_id, token):
    """Remove subscription during datasource swap"""
    url = f"{host}/api/-/pulse/subscriptions/{subscription_id}"
    headers = {"X-Tableau-Auth": token, "Content-Type": "application/json"}    
    r = requests.delete(url, headers=headers)
    r.raise_for_status()

def build_definition_payload_for_swap(definition_a, datasource_id):
    """Build definition payload for datasource swap"""
    spec = definition_a.get("specification", {})
    spec["datasource"] = {"id": datasource_id}

    payload = {
        "name": definition_a["metadata"]["name"] + "_copy",
        "specification": spec,
        "extension_options": definition_a.get("extension_options", {}),
        "representation_options": definition_a.get("representation_options", {}),
        "insights_options": definition_a.get("insights_options", {}),
        "comparisons": definition_a.get("comparisons", {}),
        "datasource_goals": definition_a.get("datasource_goals", []),
        "related_links": definition_a.get("related_links", []),
        "certification": {"is_certified": False}
    }
    return payload

# ------------------------------
# Check Certified Metrics Functions
# ------------------------------

def get_all_groups_rest(server_url, auth_token, site_id, api_version):
    """Get all groups on the site."""
    groups_url = f"{server_url}/api/{api_version}/sites/{site_id}/groups"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(groups_url, headers=headers, verify=True)
        
        if response.status_code != 200:
            return {'success': False, 'error': f"Failed to get groups. Status: {response.status_code}"}
        
        groups_data = response.json()
        groups = groups_data.get('groups', {}).get('group', [])
        
        # Handle single group response
        if isinstance(groups, dict):
            groups = [groups]
        
        group_list = []
        for group in groups:
            group_list.append({
                'id': group.get('id', ''),
                'name': group.get('name', ''),
                'domain': group.get('domain', {}).get('name', 'Local') if group.get('domain') else 'Local'
            })
        
        return {'success': True, 'groups': group_list}
        
    except Exception as e:
        return {'success': False, 'error': f"Error getting groups: {str(e)}"}

def get_users_in_group_rest(server_url, auth_token, site_id, group_id, api_version):
    """Get all users in a specific group."""
    users_url = f"{server_url}/api/{api_version}/sites/{site_id}/groups/{group_id}/users"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(users_url, headers=headers, verify=True)
        
        if response.status_code != 200:
            return {'success': False, 'error': f"Failed to get users. Status: {response.status_code}"}
        
        users_data = response.json()
        users = users_data.get('users', {}).get('user', [])
        
        # Handle single user response
        if isinstance(users, dict):
            users = [users]
        
        user_list = []
        for user in users:
            user_list.append({
                'id': user.get('id', ''),
                'name': user.get('name', ''),
                'email': user.get('email', ''),
                'site_role': user.get('siteRole', ''),
                'full_name': user.get('fullName', '')
            })
        
        return {'success': True, 'users': user_list}
        
    except Exception as e:
        return {'success': False, 'error': f"Error getting users: {str(e)}"}

def get_metric_definitions_rest(server_url, auth_token):
    """Get all metric definitions including certification status."""
    endpoint = f"{server_url}/api/-/pulse/definitions?page_size=1000"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(endpoint, headers=headers, verify=True)
        
        if response.status_code == 200:
            response_data = response.json()
            
            # Log pagination info if present
            total = response_data.get('total_available') or response_data.get('total')
            if total:
                print(f"DEBUG: Definitions API returned {total} total definitions available")
            
            return parse_metric_definitions(response_data)
        else:
            return {'success': False, 'error': f"Failed to get definitions. Status: {response.status_code}"}
            
    except Exception as e:
        return {'success': False, 'error': f"Error getting definitions: {str(e)}"}

def parse_metric_definitions(data):
    """Parse metric definitions response."""
    try:
        definitions = []
        certified_count = 0
        
        # Extract metric definitions from response
        metric_definitions = []
        if 'metric_definitions' in data:
            metric_definitions = data['metric_definitions']
        elif 'definitions' in data:
            metric_definitions = data['definitions']
        elif 'metricDefinitions' in data:
            metric_definitions = data['metricDefinitions']
        elif isinstance(data, list):
            metric_definitions = data
        
        for definition in metric_definitions:
            # Extract certification information
            certification = definition.get('certification', {})
            is_certified = certification.get('is_certified', False)
            
            if is_certified:
                certified_count += 1
            
            # Extract metadata for easy access
            metadata = definition.get('metadata', {})
            
            # Keep the full definition structure but flatten key fields for easy access
            definition_with_cert = definition.copy()
            definition_with_cert['id'] = metadata.get('id', '')
            definition_with_cert['name'] = metadata.get('name', '')
            definition_with_cert['certified'] = is_certified
            
            # Also add extracted certification details for easy access
            definition_with_cert['certification_note'] = certification.get('note', '')
            definition_with_cert['certified_by'] = certification.get('modified_by', 'Unknown')
            definition_with_cert['certified_at'] = certification.get('modified_at', '')
            definition_with_cert['certified_by_luid'] = certification.get('modified_by', '')
            
            definitions.append(definition_with_cert)
        
        print(f"DEBUG: Parsed {len(definitions)} definitions from API response")
        
        return {
            'success': True,
            'total_definitions': len(definitions),
            'certified_count': certified_count,
            'uncertified_count': len(definitions) - certified_count,
            'definitions': definitions
        }
        
    except Exception as e:
        return {'success': False, 'error': f"Error parsing definitions: {str(e)}"}

def remove_certification_rest(server_url, auth_token, definition_id):
    """Remove certification from a metric definition."""
    update_url = f"{server_url}/api/-/pulse/definitions/{definition_id}"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    request_body = {
        "certification": {
            "is_certified": False
        }
    }
    
    try:
        response = requests.patch(update_url, headers=headers, json=request_body, verify=True)
        
        if response.status_code == 200:
            return {'success': True}
        else:
            return {'success': False, 'error': f"Failed to remove certification. Status: {response.status_code}"}
            
    except Exception as e:
        return {'success': False, 'error': f"Error removing certification: {str(e)}"}

# ------------------------------
# Bulk Create Scoped Metrics Functions
# ------------------------------

def get_metric_details_rest(server_url, auth_token, metric_id):
    """Get details of a specific metric."""
    url = f"{server_url}/api/-/pulse/metrics/{metric_id}"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=True)
        
        if response.status_code == 200:
            return {'success': True, 'metric': response.json().get('metric', {})}
        else:
            return {'success': False, 'error': f"Failed to get metric. Status: {response.status_code}"}
            
    except Exception as e:
        return {'success': False, 'error': f"Error getting metric: {str(e)}"}

def get_all_metrics_for_definition_rest(server_url, auth_token, definition_id):
    """Get all metrics for a specific definition."""
    url = f"{server_url}/api/-/pulse/metrics?definition_id={definition_id}&page_size=1000"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=True)
        
        print(f"DEBUG: Metrics API response status for {definition_id}: {response.status_code}")
        
        if response.status_code == 200:
            response_data = response.json()
            metrics = response_data.get('metrics', [])
            print(f"DEBUG: Metrics API returned {len(metrics)} metrics for {definition_id}")
            if metrics:
                print(f"DEBUG: First metric ID: {metrics[0].get('id', 'Unknown')}")
            return {'success': True, 'metrics': metrics}
        else:
            print(f"DEBUG: Metrics API error: {response.text}")
            return {'success': False, 'error': f"Failed to get metrics. Status: {response.status_code}", 'response': response.text}
            
    except Exception as e:
        print(f"DEBUG: Exception getting metrics: {str(e)}")
        return {'success': False, 'error': f"Error getting metrics: {str(e)}"}

def get_all_subscriptions_rest(server_url, auth_token):
    """Get all subscriptions on the site."""
    url = f"{server_url}/api/-/pulse/subscriptions?page_size=1000"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=True)
        
        if response.status_code == 200:
            return {'success': True, 'subscriptions': response.json().get('subscriptions', [])}
        else:
            return {'success': False, 'error': f"Failed to get subscriptions. Status: {response.status_code}"}
            
    except Exception as e:
        return {'success': False, 'error': f"Error getting subscriptions: {str(e)}"}

def create_scoped_metric_rest(server_url, auth_token, definition_id, metric_specification):
    """Create a new scoped metric using getOrCreate endpoint."""
    url = f"{server_url}/api/-/pulse/metrics:getOrCreate"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    payload = {
        "definition_id": definition_id,
        "specification": metric_specification
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, verify=True)
        
        # Accept both 200 (OK) and 201 (Created) as success
        if response.status_code in [200, 201]:
            response_data = response.json()
            metric_data = response_data.get('metric', {})
            is_created = response_data.get('is_metric_created', False)
            
            return {
                'success': True, 
                'metric': metric_data,
                'is_newly_created': is_created
            }
        else:
            return {'success': False, 'error': f"Failed to create metric. Status: {response.status_code}", 'response': response.text}
            
    except Exception as e:
        return {'success': False, 'error': f"Error creating metric: {str(e)}"}

# ------------------------------
# User Preferences Functions (from Update_Pulse_User_Preferences.py)
# ------------------------------

def authenticate_tableau_rest(server_url, api_version, site_content_url, auth_method, username=None, password=None, pat_name=None, pat_token=None):
    """Authenticate using XML format and return auth data."""
    signin_url = f"{server_url}/api/{api_version}/auth/signin"
    
    try:
        if auth_method == "pat":
            xml_request = f"""<?xml version='1.0' encoding='UTF-8'?>
<tsRequest>
    <credentials personalAccessTokenName='{pat_name}' 
                personalAccessTokenSecret='{pat_token}'>
        <site contentUrl='{site_content_url}' />
    </credentials>
</tsRequest>"""
        else:  # username/password
            xml_request = f"""<?xml version='1.0' encoding='UTF-8'?>
<tsRequest>
    <credentials name='{username}' password='{password}'>
        <site contentUrl='{site_content_url}' />
    </credentials>
</tsRequest>"""
        
        headers = {
            'Content-Type': 'application/xml',
            'Accept': 'application/xml'
        }
        
        response = requests.post(signin_url, data=xml_request, headers=headers, verify=True)
        
        if response.status_code == 200:
            root = ET.fromstring(response.text)
            
            # Extract authentication token
            credentials = root.find('.//{http://tableau.com/api}credentials')
            if credentials is not None:
                auth_token = credentials.get('token')
                
                # Extract site ID
                site = credentials.find('.//{http://tableau.com/api}site')
                site_id = site.get('id') if site is not None else None
                
                # Extract user ID
                user = credentials.find('.//{http://tableau.com/api}user')
                user_id = user.get('id') if user is not None else None
                
                return {
                    'success': True,
                    'auth_token': auth_token,
                    'site_id': site_id,
                    'user_id': user_id
                }
            else:
                return {'success': False, 'error': 'Could not extract authentication token from response'}
        else:
            return {'success': False, 'error': f'Authentication failed with status code: {response.status_code}'}
            
    except Exception as e:
        return {'success': False, 'error': f'Authentication error: {str(e)}'}

def get_users_on_site(server_url, api_version, site_id, auth_token):
    """Get all users on the site."""
    all_users = []
    page_number = 1
    page_size = 100
    
    while True:
        users_url = f"{server_url}/api/{api_version}/sites/{site_id}/users?pageSize={page_size}&pageNumber={page_number}"
        
        try:
            headers = {
                'X-Tableau-Auth': auth_token,
                'Accept': 'application/json'
            }
            
            response = requests.get(users_url, headers=headers, verify=True)
            
            if response.status_code == 200:
                data = json.loads(response.text)
                
                users = data.get('users', {}).get('user', [])
                if isinstance(users, dict):
                    users = [users]
                
                users_batch = []
                for user in users:
                    user_info = {
                        'id': user.get('id', ''),
                        'name': user.get('name', ''),
                        'email': user.get('email', ''),
                        'siteRole': user.get('siteRole', ''),
                        'fullName': user.get('fullName', '')
                    }
                    users_batch.append(user_info)
                
                all_users.extend(users_batch)
                
                # Check pagination
                pagination = data.get('pagination', {})
                page_number_current = int(pagination.get('pageNumber', 1))
                page_size_current = int(pagination.get('pageSize', 100))
                total_available = int(pagination.get('totalAvailable', 0))
                
                if (page_number_current * page_size_current) >= total_available:
                    break
                
                page_number += 1
            else:
                return {'success': False, 'error': f'Failed to get users. Status: {response.status_code}'}
                
        except Exception as e:
            return {'success': False, 'error': f'Error fetching users: {str(e)}'}
    
    return {'success': True, 'users': all_users}

def find_users_by_emails(users, emails):
    """Find multiple users by their email addresses."""
    results = {}
    
    for email in emails:
        email_lower = email.lower().strip()
        found_user = None
        
        for user in users:
            if user.get('email', '').lower() == email_lower:
                found_user = user
                break
        
        results[email] = found_user
    
    return results

def build_preferences_payload(preferences, user_luid, current_user_id):
    """Transform user preferences to match the Pulse API request structure."""
    api_payload = {}
    
    # Add cadence if present
    if preferences.get('cadence'):
        api_payload['cadence'] = preferences['cadence']
    
    # Transform channel preferences
    channel_prefs_request = []
    
    if preferences.get('email_channel'):
        channel_prefs_request.append({
            'channel': 'DELIVERY_CHANNEL_EMAIL',
            'status': preferences['email_channel']
        })
    
    if preferences.get('slack_channel'):
        channel_prefs_request.append({
            'channel': 'DELIVERY_CHANNEL_SLACK',
            'status': preferences['slack_channel']
        })
    
    if channel_prefs_request:
        api_payload['channel_preferences_request'] = channel_prefs_request
    
    # Add metric grouping preferences if present
    if preferences.get('group_by') and preferences.get('sort_order'):
        api_payload['metric_grouping_preferences'] = {
            'group_by': preferences['group_by'],
            'sort_order': preferences['sort_order']
        }
    elif preferences.get('group_by'):
        api_payload['metric_grouping_preferences'] = {
            'group_by': preferences['group_by']
        }
    elif preferences.get('sort_order'):
        api_payload['metric_grouping_preferences'] = {
            'sort_order': preferences['sort_order']
        }
    
    # Add user_id for system admin capability (when updating other users)
    if user_luid and user_luid != current_user_id:
        api_payload['user_id'] = user_luid
    
    return api_payload

def update_pulse_preferences(server_url, auth_token, user_luid, preferences, current_user_id):
    """Update Pulse user preferences via REST API."""
    pulse_url = f"{server_url}/api/-/pulse/user/preferences"
    
    # Transform preferences to match the API request structure
    api_payload = build_preferences_payload(preferences, user_luid, current_user_id)
    
    if not api_payload:
        return {'success': False, 'error': 'No preferences to update'}
    
    try:
        headers = {
            'X-Tableau-Auth': auth_token,
            'Content-Type': 'application/vnd.tableau.pulse.subscriptionservice.v1.UpdateUserPreferencesRequest+json',
            'Accept': 'application/vnd.tableau.pulse.subscriptionservice.v1.UpdateUserPreferencesResponse+json'
        }
        
        response = requests.patch(pulse_url, json=api_payload, headers=headers, verify=True)
        
        if response.status_code in [200, 204]:
            return {'success': True, 'message': 'Pulse preferences updated successfully'}
        else:
            error_msg = f"Failed to update preferences. Status: {response.status_code}"
            if response.text:
                error_msg += f" Response: {response.text}"
            return {'success': False, 'error': error_msg}
            
    except Exception as e:
        return {'success': False, 'error': f'Error updating preferences: {str(e)}'}

# ------------------------------
# Tableau Cloud Manager (TCM) Functions
# ------------------------------

def tcm_login(tcm_uri, pat_token):
    """Login to Tableau Cloud Manager and get session token."""
    url = f"{tcm_uri}/api/v1/pat/login"
    
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    payload = {
        'token': pat_token
    }
    
    try:
        print(f"DEBUG: Attempting TCM login to: {url}")
        print(f"DEBUG: Payload: {json.dumps({'token': '***REDACTED***'})}")
        
        response = requests.post(url, headers=headers, json=payload, verify=True)
        
        print(f"DEBUG: TCM login response status: {response.status_code}")
        print(f"DEBUG: TCM login response headers: {dict(response.headers)}")
        
        try:
            response_json = response.json()
            print(f"DEBUG: TCM login response body: {json.dumps(response_json, indent=2)}")
        except:
            print(f"DEBUG: TCM login response body (not JSON): {response.text}")
        
        if response.status_code == 200:
            response_data = response.json()
            session_token = response_data.get('sessionToken')
            tenant_id = response_data.get('tenantId')
            
            if session_token and tenant_id:
                print(f"DEBUG: Successfully got session token and tenant_id: {tenant_id}")
                return {
                    'success': True,
                    'session_token': session_token,
                    'tenant_id': tenant_id
                }
            else:
                return {'success': False, 'error': 'Missing sessionToken or tenantId in response', 'response': response.text}
        else:
            return {
                'success': False,
                'error': f"Login failed. Status: {response.status_code}",
                'response': response.text
            }
    except Exception as e:
        tb = traceback.format_exc()
        print(f"DEBUG: Exception during TCM login: {tb}")
        return {'success': False, 'error': f"Error during TCM login: {str(e)}", 'traceback': tb}

def tcm_get_activity_log_paths(tcm_uri, session_token, tenant_id, site_id, start_time, end_time, event_type=None, max_pages=50):
    """Get list of activity log file paths - Step 1: GET request with pagination support."""
    all_file_paths = []
    page_token = None
    page_count = 0
    
    headers = {
        'x-tableau-session-token': session_token,
        'Accept': 'application/json'
    }
    
    try:
        while True:
            page_count += 1
            
            # Safety limit to prevent timeouts
            if page_count > max_pages:
                print(f"WARNING: Reached max pages limit ({max_pages}), stopping pagination")
                print(f"WARNING: Collected {len(all_file_paths)} file paths so far")
                # Mark as partial and return what we have
                return {
                    'success': True,
                    'file_paths': all_file_paths,
                    'raw_response': {'partial': True, 'message': f'Stopped at page limit ({max_pages})'},
                    'page_count': page_count,
                    'partial': True,
                    'hit_limit': True
                }
            
            # Build URL with pagination token if available - URL encode the datetime strings
            encoded_start = quote(start_time, safe='')
            encoded_end = quote(end_time, safe='')
            url = f"{tcm_uri}/api/v1/tenants/{tenant_id}/sites/{site_id}/activitylog?startTime={encoded_start}&endTime={encoded_end}"
            
            # Add eventType filter if specified
            if event_type:
                url += f"&eventType={event_type}"
            
            if page_token:
                # Don't encode pageToken - it's base64 and should be passed as-is
                url += f"&pageToken={page_token}"
            
            print(f"DEBUG: Getting activity log paths (page {page_count}): {url}")
            print(f"DEBUG: Request headers: {{'x-tableau-session-token': '***REDACTED***', 'Accept': 'application/json'}}")
            print(f"DEBUG: Session token length: {len(session_token) if session_token else 0}")
            
            # Add timeout to prevent hanging
            response = requests.get(url, headers=headers, verify=True, timeout=30)
            
            print(f"DEBUG: Get paths response status (page {page_count}): {response.status_code}")
            
            # Check for empty response or 403 on pagination (can signal end of results)
            if not response.text or response.text.strip() == '':
                print(f"DEBUG: Empty response on page {page_count}, ending pagination")
                break
            
            # If 403 on a page > 1, treat as end of pagination (some APIs do this)
            if response.status_code == 403 and page_count > 1:
                print(f"DEBUG: Got 403 on page {page_count}, treating as end of pagination")
                print(f"DEBUG: Collected {len(all_file_paths)} file paths from previous pages")
                break
            
            # Other non-200 errors on first page should fail
            if response.status_code != 200:
                return {
                    'success': False,
                    'error': f"Failed to get file paths. Status: {response.status_code}",
                    'response': response.text
                }
            
            response_data = response.json()
            
            print(f"DEBUG: Response keys on page {page_count}: {response_data.keys()}")
            
            # The response should contain file paths
            file_paths = response_data.get('filePaths', []) or response_data.get('files', []) or response_data.get('paths', [])
            
            print(f"DEBUG: File paths on page {page_count}: {len(file_paths)}")
            print(f"DEBUG: Total collected so far: {len(all_file_paths) + len(file_paths)}")
            if file_paths and page_count == 1:
                print(f"DEBUG: First file path type: {type(file_paths[0])}")
                print(f"DEBUG: First file path: {file_paths[0]}")
            
            all_file_paths.extend(file_paths)
            
            # Check for pagination token
            page_token = response_data.get('pageToken')
            if not page_token:
                print(f"DEBUG: No more pages, total file paths: {len(all_file_paths)}")
                break
            
            print(f"DEBUG: More pages available, continuing...")
        
        return {
            'success': True,
            'file_paths': all_file_paths,
            'raw_response': response_data,  # Last page response
            'page_count': page_count
        }
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timed out after page {page_count}")
        if all_file_paths:
            # Return partial results
            return {
                'success': True,
                'file_paths': all_file_paths,
                'raw_response': {'partial': True, 'message': f'Timeout after page {page_count}'},
                'page_count': page_count,
                'partial': True
            }
        else:
            return {'success': False, 'error': f"Request timed out on page {page_count}"}
    except Exception as e:
        tb = traceback.format_exc()
        print(f"DEBUG: Exception getting paths: {tb}")
        if all_file_paths:
            # Return partial results if we got some data
            return {
                'success': True,
                'file_paths': all_file_paths,
                'raw_response': {'partial': True, 'error': str(e)},
                'page_count': page_count,
                'partial': True
            }
        return {'success': False, 'error': f"Error getting activity log paths: {str(e)}"}

def tcm_get_download_urls(tcm_uri, session_token, tenant_id, site_id, file_paths):
    """Get download URLs for activity log files - Step 2: POST request."""
    url = f"{tcm_uri}/api/v1/tenants/{tenant_id}/sites/{site_id}/activitylog"
    
    headers = {
        'x-tableau-session-token': session_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # If file_paths contains dicts with a 'path' or 'file' key, extract them
    processed_paths = []
    for fp in file_paths:
        if isinstance(fp, dict):
            # It's a dict, try to get the actual path
            processed_paths.append(fp.get('path') or fp.get('file') or fp.get('filePath'))
        else:
            # It's already a string
            processed_paths.append(fp)
    
    print(f"DEBUG: Original file_paths count: {len(file_paths)}")
    print(f"DEBUG: Processed file_paths count: {len(processed_paths)}")
    if processed_paths:
        print(f"DEBUG: First processed path: {processed_paths[0]}")
    
    payload = {
        'tenantId': tenant_id,
        'files': processed_paths
    }
    
    try:
        print(f"DEBUG: Posting to get download URLs: {url}")
        print(f"DEBUG: Sending {len(file_paths)} original file paths")
        print(f"DEBUG: Processed {len(processed_paths)} file paths for POST")
        
        response = requests.post(url, headers=headers, json=payload, verify=True, timeout=60)
        
        print(f"DEBUG: Get URLs response status: {response.status_code}")
        
        if response.status_code in [200, 201, 202]:
            response_data = response.json()
            # Response should contain 'url' key with download URLs
            return {
                'success': True,
                'data': response_data
            }
        else:
            return {
                'success': False,
                'error': f"Failed to get download URLs. Status: {response.status_code}",
                'response': response.text
            }
    except Exception as e:
        tb = traceback.format_exc()
        print(f"DEBUG: Exception getting download URLs: {tb}")
        return {'success': False, 'error': f"Error getting download URLs: {str(e)}"}

def tcm_download_log_file(download_url, session_token=None):
    """Download a single activity log file - Step 3: Download from S3 pre-signed URL."""
    # S3 pre-signed URLs don't need authentication headers
    try:
        print(f"DEBUG: Downloading from URL (first 100 chars): {download_url[:100]}...")
        response = requests.get(download_url, verify=True, stream=True)
        
        if response.status_code == 200:
            # Return the content as text
            return {'success': True, 'content': response.text}
        else:
            return {
                'success': False,
                'error': f"Download failed. Status: {response.status_code}",
                'response': response.text
            }
    except Exception as e:
        return {'success': False, 'error': f"Error downloading log file: {str(e)}"}

# ------------------------------
# Flask Routes
# ------------------------------

@app.route('/')
def index():
    """Main page with Pulse Definition Copier UI"""
    return render_template('index.html')

@app.route('/api/hello')
def api_hello():
    """API endpoint that returns JSON hello message"""
    return {'message': 'Hello World from API!', 'status': 'success'}

@app.route('/copy-definitions', methods=['POST'])
def copy_definitions():
    """Handle the form submission and copy pulse definitions"""
    try:
        data = request.get_json()
        results = []
        
        # Extract form data
        source_host = data.get('source_host', '').strip()
        source_content_url = data.get('source_content_url', '').strip()
        source_auth_method = data.get('source_auth_method')
        source_datasource = data.get('source_datasource', '').strip()
        
        dest_host = data.get('dest_host', '').strip()
        dest_content_url = data.get('dest_content_url', '').strip()
        dest_auth_method = data.get('dest_auth_method')
        dest_datasource = data.get('dest_datasource', '').strip()
        
        definition_ids = data.get('definition_ids', '').strip() or 'all'
        
        # Validate required fields
        required_fields = [source_host, source_content_url, source_datasource, 
                          dest_host, dest_content_url, dest_datasource]
        if not all(required_fields):
            return jsonify({
                'success': False,
                'error': 'All host, content URL, and datasource fields are required'
            })
        
        # Sign in to source site
        try:
            if source_auth_method == 'u':
                source_username = data.get('source_username', '').strip()
                source_password = data.get('source_password', '').strip()
                if not source_username or not source_password:
                    return jsonify({'success': False, 'error': 'Source username and password are required'})
                token_a, site_id_a = sign_in_rest(source_host, source_content_url, source_username, source_password)
            elif source_auth_method == 'p':
                source_pat_name = data.get('source_pat_name', '').strip()
                source_pat_secret = data.get('source_pat_secret', '').strip()
                if not source_pat_name or not source_pat_secret:
                    return jsonify({'success': False, 'error': 'Source PAT name and secret are required'})
                token_a, site_id_a = sign_in_rest(source_host, source_content_url, 
                                                 pat_name=source_pat_name, pat_secret=source_pat_secret)
            else:
                return jsonify({'success': False, 'error': 'Invalid source authentication method'})
        except Exception as e:
            return jsonify({'success': False, 'error': f'Source authentication failed: {str(e)}'})
        
        results.append({'success': True, 'message': f'✅ Signed in to source site'})
        
        # Sign in to destination site
        try:
            if dest_auth_method == 'u':
                dest_username = data.get('dest_username', '').strip()
                dest_password = data.get('dest_password', '').strip()
                if not dest_username or not dest_password:
                    return jsonify({'success': False, 'error': 'Destination username and password are required'})
                token_b, site_id_b = sign_in_rest(dest_host, dest_content_url, dest_username, dest_password)
            elif dest_auth_method == 'p':
                dest_pat_name = data.get('dest_pat_name', '').strip()
                dest_pat_secret = data.get('dest_pat_secret', '').strip()
                if not dest_pat_name or not dest_pat_secret:
                    return jsonify({'success': False, 'error': 'Destination PAT name and secret are required'})
                token_b, site_id_b = sign_in_rest(dest_host, dest_content_url, 
                                                 pat_name=dest_pat_name, pat_secret=dest_pat_secret)
            else:
                return jsonify({'success': False, 'error': 'Invalid destination authentication method'})
        except Exception as e:
            force_sign_out(source_host, token_a)
            return jsonify({'success': False, 'error': f'Destination authentication failed: {str(e)}'})
        
        results.append({'success': True, 'message': f'✅ Signed in to destination site'})
        
        # Get datasource IDs
        try:
            datasource_id_a = get_datasource_id_rest(source_host, token_a, site_id_a, source_datasource)
            results.append({'success': True, 'message': f'✅ Found source datasource: {source_datasource}'})
        except Exception as e:
            force_sign_out(source_host, token_a)
            force_sign_out(dest_host, token_b)
            return jsonify({'success': False, 'error': f'Source datasource lookup failed: {str(e)}'})
        
        try:
            datasource_id_b = get_datasource_id_rest(dest_host, token_b, site_id_b, dest_datasource)
            results.append({'success': True, 'message': f'✅ Found destination datasource: {dest_datasource}'})
        except Exception as e:
            force_sign_out(source_host, token_a)
            force_sign_out(dest_host, token_b)
            return jsonify({'success': False, 'error': f'Destination datasource lookup failed: {str(e)}'})
        
        # Get definitions to copy
        try:
            definition_ids_to_copy = get_definitions_to_copy(source_host, token_a, datasource_id_a, definition_ids)
            if not definition_ids_to_copy:
                force_sign_out(source_host, token_a)
                force_sign_out(dest_host, token_b)
                return jsonify({'success': False, 'error': 'No definitions found to copy'})
            
            results.append({'success': True, 'message': f'✅ Found {len(definition_ids_to_copy)} definition(s) to copy'})
        except Exception as e:
            force_sign_out(source_host, token_a)
            force_sign_out(dest_host, token_b)
            return jsonify({'success': False, 'error': f'Definition lookup failed: {str(e)}'})
        
        # Copy each definition
        copied_count = 0
        failed_count = 0
        
        for def_id in definition_ids_to_copy:
            try:
                # Get source definition
                definition_a = get_pulse_definition(source_host, def_id, token_a)
                def_name = definition_a['metadata']['name']
                
                # Build payload for destination
                payload = build_definition_payload(definition_a, datasource_id_b)
                
                # Create on destination
                new_definition = create_pulse_definition(dest_host, token_b, payload)
                
                if new_definition and "definition" in new_definition and "metadata" in new_definition["definition"]:
                    results.append({'success': True, 'message': f'✅ Created: {def_name}'})
                    copied_count += 1
                else:
                    results.append({'success': False, 'message': f'❌ Failed to create: {def_name}'})
                    failed_count += 1
                    
            except Exception as e:
                results.append({'success': False, 'message': f'❌ Error copying definition {def_id}: {str(e)}'})
                failed_count += 1
        
        # Sign out
        force_sign_out(source_host, token_a)
        force_sign_out(dest_host, token_b)
        
        # Prepare response
        summary = f"Completed! {copied_count} definitions copied successfully"
        if failed_count > 0:
            summary += f", {failed_count} failed"
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary,
            'copied_count': copied_count,
            'failed_count': failed_count
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        })

@app.route('/manage-followers', methods=['POST'])
def manage_followers():
    """Handle bulk manage followers form submission"""
    try:
        data = request.get_json()
        results = []
        
        # Extract form data
        server_host = data.get('server_host', '').strip()
        site_content_url = data.get('site_content_url', '').strip()
        auth_method = data.get('auth_method')
        action = data.get('action')  # 'add' or 'remove'
        metric_ids = data.get('metric_ids', '').strip()
        user_emails_raw = data.get('user_emails', '').strip()
        
        # Validate required fields
        if not all([server_host, site_content_url, auth_method, action, metric_ids, user_emails_raw]):
            return jsonify({
                'success': False,
                'error': 'All fields are required'
            })
        
        if action not in ['add', 'remove']:
            return jsonify({
                'success': False,
                'error': 'Invalid action. Must be "add" or "remove"'
            })
        
        # Parse metric IDs and user emails
        metrics = [m.strip() for m in metric_ids.split(",") if m.strip()]
        user_emails = [u.strip() for u in user_emails_raw.replace('\n', ',').split(',') if u.strip()]
        
        # Sign in to server
        try:
            if auth_method == 'password':
                username = data.get('username', '').strip()
                password = data.get('password', '').strip()
                if not username or not password:
                    return jsonify({'success': False, 'error': 'Username and password are required'})
                rest_token, site_id = sign_in_rest_xml(server_host, site_content_url, "password", 
                                                     username=username, password=password)
            elif auth_method == 'pat':
                pat_name = data.get('pat_name', '').strip()
                pat_token = data.get('pat_token', '').strip()
                if not pat_name or not pat_token:
                    return jsonify({'success': False, 'error': 'PAT name and token are required'})
                rest_token, site_id = sign_in_rest_xml(server_host, site_content_url, "pat", 
                                                     pat_name=pat_name, pat_token=pat_token)
            else:
                return jsonify({'success': False, 'error': 'Invalid authentication method'})
        except Exception as e:
            return jsonify({'success': False, 'error': f'Authentication failed: {str(e)}'})
        
        results.append({'success': True, 'message': '✅ Signed in successfully'})
        
        # Convert emails to user IDs
        user_ids = []
        for email in user_emails:
            try:
                uid = get_user_id_by_email(server_host, rest_token, site_id, email)
                results.append({'success': True, 'message': f'✅ Found user: {email} → {uid}'})
                user_ids.append(uid)
            except Exception as e:
                results.append({'success': False, 'message': f'❌ User not found: {email} - {str(e)}'})
        
        if not user_ids:
            return jsonify({'success': False, 'error': 'No valid users found'})
        
        # Process each metric
        successful_operations = 0
        failed_operations = 0
        
        for metric_id in metrics:
            try:
                existing_followers = get_metric_followers(server_host, rest_token, metric_id)
                
                if action == 'add':
                    to_add = [uid for uid in user_ids if uid not in existing_followers]
                    if to_add:
                        result = batch_create_subscriptions(server_host, rest_token, metric_id, to_add)
                        results.append(result)
                        successful_operations += 1
                    else:
                        results.append({'success': True, 'message': f'ℹ️ All users already follow metric {metric_id}'})
                        
                else:  # remove
                    user_ids_to_remove = [uid for uid in user_ids if uid in existing_followers]
                    if user_ids_to_remove:
                        result = remove_followers(server_host, rest_token, metric_id, user_ids_to_remove)
                        results.append(result)
                        successful_operations += 1
                    else:
                        results.append({'success': True, 'message': f'ℹ️ None of the users follow metric {metric_id}'})
                        
            except Exception as e:
                results.append({'success': False, 'message': f'❌ Failed to process metric {metric_id}: {str(e)}'})
                failed_operations += 1
        
        # Sign out
        force_sign_out(server_host, rest_token)
        
        # Prepare response
        action_word = "added to" if action == "add" else "removed from"
        summary = f"Completed! Users {action_word} {successful_operations} metrics successfully"
        if failed_operations > 0:
            summary += f", {failed_operations} failed"
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary,
            'successful_operations': successful_operations,
            'failed_operations': failed_operations
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        })

@app.route('/swap-datasources', methods=['POST'])
def swap_datasources():
    """Handle swap datasources form submission"""
    try:
        data = request.get_json()
        results = []
        
        # Extract form data
        server_host = data.get('server_host', '').strip()
        site_content_url = data.get('site_content_url', '').strip()
        auth_method = data.get('auth_method')
        definition_id = data.get('definition_id', '').strip()
        new_datasource_id = data.get('new_datasource_id', '').strip()
        remove_old_followers = data.get('remove_old_followers') == 'true'
        
        # Validate required fields
        if not all([server_host, site_content_url, auth_method, definition_id, new_datasource_id]):
            return jsonify({
                'success': False,
                'error': 'All fields are required'
            })
        
        # Sign in to server using JSON auth (consistent with original swap script)
        try:
            if auth_method == 'password':
                username = data.get('username', '').strip()
                password = data.get('password', '').strip()
                if not username or not password:
                    return jsonify({'success': False, 'error': 'Username and password are required'})
                token, site_id = sign_in_rest(server_host, site_content_url, username=username, password=password)
            elif auth_method == 'pat':
                pat_name = data.get('pat_name', '').strip()
                pat_secret = data.get('pat_secret', '').strip()
                if not pat_name or not pat_secret:
                    return jsonify({'success': False, 'error': 'PAT name and secret are required'})
                token, site_id = sign_in_rest(server_host, site_content_url, 
                                             pat_name=pat_name, pat_secret=pat_secret)
            else:
                return jsonify({'success': False, 'error': 'Invalid authentication method'})
        except Exception as e:
            return jsonify({'success': False, 'error': f'Authentication failed: {str(e)}'})
        
        results.append({'success': True, 'message': '✅ Signed in successfully'})
        
        # Copy Definition
        try:
            old_def = get_pulse_definition_for_swap(server_host, definition_id, token)
            payload = build_definition_payload_for_swap(old_def, new_datasource_id)
            new_def = create_pulse_definition_for_swap(server_host, token, payload)
            
            new_def_id = new_def["metadata"]["id"]
            new_def_name = new_def.get("metadata", {}).get("name")
            
            results.append({'success': True, 'message': f'✅ Created new definition: {new_def_name} (ID: {new_def_id})'})
        except Exception as e:
            force_sign_out(server_host, token)
            return jsonify({'success': False, 'error': f'Failed to copy definition: {str(e)}'})
        
        # Copy Metrics + Followers
        try:
            old_metrics = get_metrics_for_definition_swap(server_host, definition_id, token)
            results.append({'success': True, 'message': f'➡ Found {len(old_metrics)} metrics to copy'})
            
            copied_metrics = 0
            copied_followers = 0
            
            for m in old_metrics:
                # Skip the default metric
                old_metric_id = m.get("id") or m.get("metadata", {}).get("id")
                if m.get("is_default", False):
                    results.append({'success': True, 'message': f'➡ Skipping default metric: {m.get("metadata", {}).get("name", "<unknown>")}'})
                    continue

                metric_payload = {
                    "definition_id": new_def_id,
                    "specification": m.get("specification", {})
                }

                try:
                    new_metric = create_metric_for_swap(server_host, new_def_id, metric_payload, token)
                    new_metric_id = new_metric.get("metric", {}).get("id")
                    metric_name = m.get("metadata", {}).get("name", "<unknown>")
                    results.append({'success': True, 'message': f'✅ Created metric: {metric_name}'})
                    copied_metrics += 1

                    # Copy followers (subscriptions)
                    if old_metric_id and new_metric_id:
                        subscriptions = get_subscriptions_for_swap(server_host, old_metric_id, token)
                        
                        if subscriptions:
                            for sub in subscriptions:
                                user_id = sub["follower"]["user_id"]
                                try:
                                    add_follower_for_swap(server_host, new_metric_id, user_id, token)
                                    copied_followers += 1
                                except Exception as e:
                                    results.append({'success': False, 'message': f'⚠️ Failed to copy follower {user_id}: {str(e)}'})
                            
                            results.append({'success': True, 'message': f'✅ Copied {len(subscriptions)} followers to metric {metric_name}'})
                        else:
                            results.append({'success': True, 'message': f'ℹ️ No followers found for metric {metric_name}'})

                except Exception as e:
                    results.append({'success': False, 'message': f'❌ Failed to create metric {metric_name}: {str(e)}'})

        except Exception as e:
            force_sign_out(server_host, token)
            return jsonify({'success': False, 'error': f'Failed to copy metrics: {str(e)}'})
        
        # Optionally remove old followers
        if remove_old_followers:
            try:
                results.append({'success': True, 'message': '🧹 Removing followers from old metrics...'})
                
                for m in old_metrics:
                    metric_id = m.get("id")
                    metric_name = m.get("metadata", {}).get("name", "<unknown>")

                    if not metric_id:
                        continue

                    try:
                        subscriptions = get_subscriptions_for_swap(server_host, metric_id, token)
                        for s in subscriptions:
                            sub_id = s["id"]
                            remove_subscription_for_swap(server_host, sub_id, token)
                        results.append({'success': True, 'message': f'✅ Removed followers from metric {metric_name}'})
                    except Exception as e:
                        results.append({'success': False, 'message': f'⚠️ Failed to remove followers from metric {metric_name}: {str(e)}'})
                        
            except Exception as e:
                results.append({'success': False, 'message': f'⚠️ Error during cleanup: {str(e)}'})
        
        # Sign out
        force_sign_out(server_host, token)
        
        # Prepare response
        summary = f"Completed! Created new definition with {copied_metrics} metrics and {copied_followers} followers"
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary,
            'copied_metrics': copied_metrics,
            'copied_followers': copied_followers,
            'new_definition_id': new_def_id
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        })

@app.route('/update-preferences', methods=['POST'])
def update_preferences():
    """Update Tableau Pulse user preferences for single or multiple users"""
    try:
        data = request.json
        
        # Extract form data
        server_url = data.get('server_url', '').rstrip('/')
        api_version = data.get('api_version', '3.26')
        site_content_url = data.get('site_content_url', '')
        auth_method = data.get('auth_method')
        user_emails_input = data.get('user_emails', '')
        
        # Authentication data
        username = data.get('username')
        password = data.get('password')
        pat_name = data.get('pat_name')
        pat_token = data.get('pat_token')
        
        # Preferences data
        preferences = {}
        if data.get('cadence'):
            preferences['cadence'] = data.get('cadence')
        if data.get('email_channel'):
            preferences['email_channel'] = data.get('email_channel')
        if data.get('slack_channel'):
            preferences['slack_channel'] = data.get('slack_channel')
        if data.get('group_by'):
            preferences['group_by'] = data.get('group_by')
        if data.get('sort_order'):
            preferences['sort_order'] = data.get('sort_order')
        
        # Validate required fields
        if not all([server_url, auth_method, user_emails_input]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: server_url, auth_method, and user_emails are required'
            })
        
        # Validate authentication fields
        if auth_method == 'pat':
            if not all([pat_name, pat_token]):
                return jsonify({
                    'success': False,
                    'error': 'PAT authentication requires both pat_name and pat_token'
                })
        else:
            if not all([username, password]):
                return jsonify({
                    'success': False,
                    'error': 'Password authentication requires both username and password'
                })
        
        # Check if any preferences are configured
        if not preferences:
            return jsonify({
                'success': False,
                'error': 'No preferences configured. Please select at least one preference to update.'
            })
        
        # Parse user emails
        emails = []
        for email in user_emails_input.replace('\n', ',').split(','):
            email = email.strip()
            if email:
                emails.append(email)
        
        if not emails:
            return jsonify({
                'success': False,
                'error': 'No valid email addresses provided'
            })
        
        results = []
        results.append({'success': True, 'message': f'🚀 Starting preferences update for {len(emails)} user(s)...'})
        
        # Authenticate
        results.append({'success': True, 'message': '🔐 Authenticating with Tableau Server...'})
        
        auth_result = authenticate_tableau_rest(
            server_url, api_version, site_content_url, auth_method,
            username, password, pat_name, pat_token
        )
        
        if not auth_result['success']:
            return jsonify({
                'success': False,
                'error': f"Authentication failed: {auth_result['error']}"
            })
        
        auth_token = auth_result['auth_token']
        site_id = auth_result['site_id']
        current_user_id = auth_result['user_id']
        
        results.append({'success': True, 'message': '✅ Authentication successful!'})
        
        # Get all users on site
        results.append({'success': True, 'message': '👥 Fetching users from site...'})
        
        users_result = get_users_on_site(server_url, api_version, site_id, auth_token)
        
        if not users_result['success']:
            return jsonify({
                'success': False,
                'error': f"Failed to get users: {users_result['error']}"
            })
        
        users = users_result['users']
        results.append({'success': True, 'message': f'📊 Found {len(users)} users on site'})
        
        # Find users by emails
        results.append({'success': True, 'message': f'🔍 Looking up {len(emails)} user(s) by email...'})
        
        user_lookup_results = find_users_by_emails(users, emails)
        
        found_users = []
        not_found_users = []
        
        for email, user in user_lookup_results.items():
            if user:
                found_users.append({
                    'email': email,
                    'luid': user['id'],
                    'name': user.get('name', 'Unknown'),
                    'user': user
                })
            else:
                not_found_users.append(email)
        
        # Report lookup results
        if found_users:
            results.append({'success': True, 'message': f'✅ Found {len(found_users)} user(s)'})
        
        if not_found_users:
            results.append({'success': False, 'message': f'❌ Not found: {", ".join(not_found_users)}'})
        
        if not found_users:
            return jsonify({
                'success': False,
                'error': 'No users found. Cannot proceed with preferences update.'
            })
        
        # Update preferences for each found user
        results.append({'success': True, 'message': f'⚙️ Updating preferences for {len(found_users)} user(s)...'})
        
        successful_updates = []
        failed_updates = []
        
        for i, user_info in enumerate(found_users, 1):
            email = user_info['email']
            user_luid = user_info['luid']
            user_name = user_info['name']
            
            results.append({'success': True, 'message': f'[{i}/{len(found_users)}] 🔄 Updating {user_name} ({email})...'})
            
            try:
                update_result = update_pulse_preferences(
                    server_url, auth_token, user_luid, preferences, current_user_id
                )
                
                if update_result['success']:
                    successful_updates.append(user_info)
                    results.append({'success': True, 'message': f'[{i}/{len(found_users)}] ✅ {user_name} - preferences updated successfully'})
                else:
                    failed_updates.append(user_info)
                    results.append({'success': False, 'message': f'[{i}/{len(found_users)}] ❌ {user_name} - {update_result["error"]}'})
                
            except Exception as e:
                failed_updates.append(user_info)
                results.append({'success': False, 'message': f'[{i}/{len(found_users)}] ❌ {user_name} - Exception: {str(e)}'})
        
        # Final summary
        results.append({'success': True, 'message': '=' * 60})
        results.append({'success': True, 'message': '📊 UPDATE SUMMARY'})
        results.append({'success': True, 'message': '=' * 60})
        results.append({'success': True, 'message': f'✅ Successful updates: {len(successful_updates)}'})
        results.append({'success': True, 'message': f'❌ Failed updates: {len(failed_updates)}'})
        results.append({'success': True, 'message': f'👥 Total processed: {len(found_users)}'})
        
        if successful_updates:
            results.append({'success': True, 'message': '✅ Successfully updated:'})
            for user_info in successful_updates:
                results.append({'success': True, 'message': f'   • {user_info["name"]} ({user_info["email"]})'})
        
        if failed_updates:
            results.append({'success': True, 'message': '❌ Failed to update:'})
            for user_info in failed_updates:
                results.append({'success': False, 'message': f'   • {user_info["name"]} ({user_info["email"]})'})
        
        if not_found_users:
            results.append({'success': True, 'message': '⚠️ Users not found (skipped):'})
            for email in not_found_users:
                results.append({'success': True, 'message': f'   • {email}'})
        
        results.append({'success': True, 'message': '🎉 Preferences update completed!'})
        
        # Summary for response
        summary = f"Updated preferences for {len(successful_updates)}/{len(found_users)} users"
        if not_found_users:
            summary += f" ({len(not_found_users)} not found)"
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary,
            'successful_updates': len(successful_updates),
            'failed_updates': len(failed_updates),
            'not_found_users': len(not_found_users)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        })

@app.route('/check-certified-metrics', methods=['POST'])
def check_certified_metrics():
    """Check certified metrics and optionally remove certifications"""
    try:
        data = request.json
        results = []
        
        # Extract form data
        server_url = data.get('server_url', '').rstrip('/')
        api_version = data.get('api_version', '3.26')
        site_content_url = data.get('site_content_url', '')
        auth_method = data.get('auth_method')
        group_name = data.get('group_name', '').strip()
        remove_non_group_certs = data.get('remove_non_group_certs') == 'true'
        
        # Authentication data
        username = data.get('username')
        password = data.get('password')
        pat_name = data.get('pat_name')
        pat_token = data.get('pat_token')
        
        # Validate required fields
        if not all([server_url, auth_method]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: server_url and auth_method are required'
            })
        
        # Authenticate
        results.append({'success': True, 'message': '🔐 Authenticating with Tableau Server...'})
        
        auth_result = authenticate_tableau_rest(
            server_url, api_version, site_content_url, auth_method,
            username, password, pat_name, pat_token
        )
        
        if not auth_result['success']:
            return jsonify({
                'success': False,
                'error': f"Authentication failed: {auth_result['error']}"
            })
        
        auth_token = auth_result['auth_token']
        site_id = auth_result['site_id']
        
        results.append({'success': True, 'message': '✅ Authentication successful!'})
        
        # Get all groups
        results.append({'success': True, 'message': '👥 Getting all groups...'})
        groups_result = get_all_groups_rest(server_url, auth_token, site_id, api_version)
        
        if not groups_result['success']:
            return jsonify({
                'success': False,
                'error': f"Failed to get groups: {groups_result['error']}"
            })
        
        all_groups = groups_result['groups']
        results.append({'success': True, 'message': f'✅ Found {len(all_groups)} groups on the site'})
        
        # Look up group ID from group name if provided
        group_id = None
        group_users = []
        if group_name:
            results.append({'success': True, 'message': f'🔍 Looking up group: {group_name}...'})
            
            # Find group by name (case-insensitive)
            matching_group = None
            for group in all_groups:
                if group['name'].lower() == group_name.lower():
                    matching_group = group
                    break
            
            if not matching_group:
                return jsonify({
                    'success': False,
                    'error': f"Group '{group_name}' not found. Please check the group name and try again."
                })
            
            group_id = matching_group['id']
            results.append({'success': True, 'message': f'✅ Found group: {matching_group["name"]} (ID: {group_id})'})
            
            # Get users in the specified group
            results.append({'success': True, 'message': f'👥 Getting users in group "{matching_group["name"]}"...'})
            users_result = get_users_in_group_rest(server_url, auth_token, site_id, group_id, api_version)
            
            if not users_result['success']:
                return jsonify({
                    'success': False,
                    'error': f"Failed to get group users: {users_result['error']}"
                })
            
            group_users = users_result['users']
            results.append({'success': True, 'message': f'✅ Found {len(group_users)} users in group "{matching_group["name"]}"'})
        
        # Get metric definitions
        results.append({'success': True, 'message': '📊 Getting metric definitions...'})
        definitions_result = get_metric_definitions_rest(server_url, auth_token)
        
        if not definitions_result['success']:
            return jsonify({
                'success': False,
                'error': f"Failed to get metric definitions: {definitions_result['error']}"
            })
        
        total_defs = definitions_result['total_definitions']
        certified_count = definitions_result['certified_count']
        uncertified_count = definitions_result['uncertified_count']
        definitions = definitions_result['definitions']
        
        results.append({'success': True, 'message': f'📊 Found {total_defs} metric definitions'})
        results.append({'success': True, 'message': f'✅ Certified: {certified_count}'})
        results.append({'success': True, 'message': f'❌ Uncertified: {uncertified_count}'})
        
        # Find metrics certified by group members vs non-group members
        if group_name and group_users:
            group_luids = {user.get('id', '') for user in group_users}
            group_certified = [d for d in definitions if d.get('certified', False) and d.get('certified_by_luid', '') in group_luids]
            non_group_certified = [d for d in definitions if d.get('certified', False) and d.get('certified_by_luid', '') not in group_luids]
            
            results.append({'success': True, 'message': f'👥 Certified by group members: {len(group_certified)}'})
            results.append({'success': True, 'message': f'⚠️ Certified by non-group members: {len(non_group_certified)}'})
            
            # List certified metrics
            results.append({'success': True, 'message': '\n📋 CERTIFIED METRICS:'})
            results.append({'success': True, 'message': '=' * 60})
            
            for definition in [d for d in definitions if d.get('certified', False)]:
                certifier_luid = definition.get('certified_by_luid', '')
                group_status = "✅ IN GROUP" if certifier_luid in group_luids else "❌ NOT IN GROUP"
                
                results.append({
                    'success': True,
                    'message': f"📊 {definition['name']}",
                    'metadata': {
                        'id': definition['id'],
                        'certified_by': definition['certified_by'],
                        'group_status': group_status,
                        'certified_at': definition.get('certified_at', ''),
                        'in_group': certifier_luid in group_luids
                    }
                })
            
            # Remove certifications if requested
            if remove_non_group_certs and non_group_certified:
                results.append({'success': True, 'message': f'\n🗑️ Removing {len(non_group_certified)} certifications from non-group members...'})
                
                success_count = 0
                for definition in non_group_certified:
                    remove_result = remove_certification_rest(server_url, auth_token, definition['id'])
                    if remove_result['success']:
                        results.append({'success': True, 'message': f"✅ Removed certification from: {definition['name']}"})
                        success_count += 1
                    else:
                        results.append({'success': False, 'message': f"❌ Failed to remove certification from: {definition['name']}"})
                
                results.append({'success': True, 'message': f'\n📊 Removed {success_count}/{len(non_group_certified)} certifications'})
        else:
            # List all certified metrics without group filtering
            results.append({'success': True, 'message': '\n📋 CERTIFIED METRICS:'})
            results.append({'success': True, 'message': '=' * 60})
            
            for definition in [d for d in definitions if d.get('certified', False)]:
                results.append({
                    'success': True,
                    'message': f"📊 {definition['name']}",
                    'metadata': {
                        'id': definition['id'],
                        'certified_by': definition['certified_by'],
                        'certified_at': definition.get('certified_at', '')
                    }
                })
        
        # Summary
        summary = f"Found {total_defs} metric definitions ({certified_count} certified, {uncertified_count} uncertified)"
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary,
            'groups': all_groups,
            'total_definitions': total_defs,
            'certified_count': certified_count,
            'uncertified_count': uncertified_count
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}'
        })

@app.route('/bulk-create-scoped-metrics', methods=['POST'])
def bulk_create_scoped_metrics():
    """Create multiple scoped metrics from CSV: dimension name, filter values (comma-sep), followers (comma-sep emails)"""
    try:
        results = []
        
        # Check if this is a CSV file upload
        if 'csv_file' in request.files:
            # CSV Upload Mode
            csv_file = request.files['csv_file']
            
            if csv_file.filename == '':
                return jsonify({'success': False, 'error': 'No CSV file selected'}), 400
            
            # Extract form data from multipart
            server_url = request.form.get('server_url', '').rstrip('/')
            api_version = request.form.get('api_version', '3.26')
            site_content_url = request.form.get('site_content_url', '')
            auth_method = request.form.get('auth_method')
            source_metric_id = request.form.get('source_metric_id', '').strip()
            
            # Authentication data
            username = request.form.get('username')
            password = request.form.get('password')
            pat_name = request.form.get('pat_name')
            pat_token = request.form.get('pat_token')
            
            # Parse CSV
            csv_content = csv_file.read().decode('utf-8')
            csv_reader = csv.reader(io.StringIO(csv_content))
            rows = list(csv_reader)
            
            if not rows:
                return jsonify({'success': False, 'error': 'CSV file is empty'}), 400
            
            # Check if first row looks like a header
            first_row = rows[0]
            if len(first_row) >= 2 and any(keyword in first_row[0].lower() for keyword in ['dimension', 'name', 'field', 'column']):
                rows = rows[1:]  # Skip header
            
            # Parse CSV rows into metric definitions
            metric_definitions = []
            for row_num, row in enumerate(rows, start=1):
                if len(row) < 2:
                    return jsonify({
                        'success': False,
                        'error': f'Row {row_num} must have at least 2 columns: dimension name and filter values'
                    }), 400
                
                dimension_name = row[0].strip()
                filter_values_text = row[1].strip()
                followers_text = row[2].strip() if len(row) > 2 else ''
                
                if not dimension_name or not filter_values_text:
                    return jsonify({
                        'success': False,
                        'error': f'Row {row_num} has empty dimension name or filter values'
                    }), 400
                
                # Parse comma-separated filter values
                filter_values = [v.strip() for v in filter_values_text.split(',') if v.strip()]
                
                # Parse comma-separated follower emails
                followers = [f.strip() for f in followers_text.split(',') if f.strip()]
                
                metric_definitions.append({
                    'dimension_name': dimension_name,
                    'filter_values': filter_values,
                    'followers': followers
                })
            
            if not metric_definitions:
                return jsonify({'success': False, 'error': 'No valid data rows found in CSV'}), 400
                
        else:
            # Legacy JSON Mode (manual form input)
            data = request.json
            
            server_url = data.get('server_url', '').rstrip('/')
            api_version = data.get('api_version', '3.26')
            site_content_url = data.get('site_content_url', '')
            auth_method = data.get('auth_method')
            source_metric_id = data.get('source_metric_id', '').strip()
            dimension_name = data.get('dimension_name', '').strip()
            dimension_values_raw = data.get('dimension_values', '').strip()
            
            # Authentication data
            username = data.get('username')
            password = data.get('password')
            pat_name = data.get('pat_name')
            pat_token = data.get('pat_token')
            
            # Validate required fields
            if not all([server_url, auth_method, source_metric_id, dimension_name, dimension_values_raw]):
                return jsonify({
                    'success': False,
                    'error': 'Missing required fields'
                })
            
            # Parse as single dimension values (legacy mode - one value per metric)
            dimension_values = [v.strip() for v in dimension_values_raw.split(',') if v.strip()]
            
            if not dimension_values:
                return jsonify({'success': False, 'error': 'No valid dimension values provided'})
            
            # Convert to metric definitions format (single filter value per metric, no followers)
            metric_definitions = [
                {
                    'dimension_name': dimension_name,
                    'filter_values': [value],
                    'followers': []
                }
                for value in dimension_values
            ]
        
        # Validate common required fields
        if not all([server_url, auth_method, source_metric_id]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: server_url, auth_method, and source_metric_id are required'
            })
        
        results.append({'success': True, 'message': f'🚀 Starting bulk scoped metric creation...'})
        results.append({'success': True, 'message': f'📊 Source Metric ID: {source_metric_id}'})
        results.append({'success': True, 'message': f'📋 Creating {len(metric_definitions)} scoped metric(s)'})
        
        # Authenticate
        results.append({'success': True, 'message': '🔐 Authenticating with Tableau Server...'})
        
        auth_result = authenticate_tableau_rest(
            server_url, api_version, site_content_url, auth_method,
            username, password, pat_name, pat_token
        )
        
        if not auth_result['success']:
            return jsonify({
                'success': False,
                'error': f"Authentication failed: {auth_result['error']}"
            })
        
        auth_token = auth_result['auth_token']
        site_id = auth_result.get('site_id', '')
        results.append({'success': True, 'message': '✅ Authentication successful!'})
        
        # Get source metric details
        results.append({'success': True, 'message': f'📊 Retrieving source metric details...'})
        metric_result = get_metric_details_rest(server_url, auth_token, source_metric_id)
        
        if not metric_result['success']:
            return jsonify({
                'success': False,
                'error': f"Failed to get source metric: {metric_result['error']}"
            })
        
        source_metric = metric_result['metric']
        definition_id = source_metric.get('definition_id')
        source_specification = source_metric.get('specification', {})
        
        if not definition_id:
            return jsonify({
                'success': False,
                'error': 'Could not determine definition_id from source metric'
            })
        
        results.append({'success': True, 'message': f'✅ Retrieved source metric (Definition: {definition_id})'})
        
        # Get existing filters count from source metric
        existing_filters_count = len(source_specification.get('filters', []))
        results.append({'success': True, 'message': f'📋 Source metric has {existing_filters_count} existing filter(s)'})
        
        # Create scoped metrics from definitions
        created_count = 0
        failed_count = 0
        followers_added_count = 0
        followers_failed_count = 0
        
        results.append({'success': True, 'message': f'\n🔄 Creating scoped metrics...'})
        
        for i, metric_def in enumerate(metric_definitions, 1):
            try:
                dimension_name = metric_def['dimension_name']
                filter_values = metric_def['filter_values']
                followers = metric_def['followers']
                
                # Deep copy the source specification to avoid any reference issues
                new_specification = copy.deepcopy(source_specification)
                
                # Get existing filters (or empty list if none)
                new_filters = new_specification.get('filters', [])
                
                # Add a filter for this dimension with ALL the specified values
                # Using OPERATOR_IN for multiple values, OPERATOR_EQUAL for single value
                if len(filter_values) == 1:
                    new_filter = {
                        "field": dimension_name,
                        "operator": "OPERATOR_EQUAL",
                        "categorical_values": [{"string_value": filter_values[0]}]
                    }
                else:
                    new_filter = {
                        "field": dimension_name,
                        "operator": "OPERATOR_IN",
                        "categorical_values": [{"string_value": val} for val in filter_values]
                    }
                
                new_filters.append(new_filter)
                
                # Update the specification with new filters
                new_specification['filters'] = new_filters
                
                # Remove comparison field entirely - it's not needed for getOrCreate
                if 'comparison' in new_specification:
                    del new_specification['comparison']
                
                # Create a readable description of the filter
                filter_desc = f"{dimension_name}={', '.join(filter_values)}"
                
                # Log what we're about to send (for debugging)
                print(f"Creating metric {i}/{len(metric_definitions)}: {filter_desc}")
                print(f"New specification: {json.dumps(new_specification, indent=2)}")
                
                # Create the scoped metric
                create_result = create_scoped_metric_rest(server_url, auth_token, definition_id, new_specification)
                
                if create_result['success']:
                    new_metric = create_result['metric']
                    new_metric_id = new_metric.get('id', 'Unknown')
                    is_newly_created = create_result.get('is_newly_created', False)
                    
                    # Show different message for newly created vs. already existing
                    status = "✨ Created" if is_newly_created else "✅ Found existing"
                    results.append({'success': True, 'message': f'[{i}/{len(metric_definitions)}] {status}: {filter_desc} (ID: {new_metric_id})'})
                    
                    created_count += 1
                    
                    # Add followers if specified
                    if followers:
                        results.append({'success': True, 'message': f'  👥 Adding {len(followers)} follower(s)...'})
                        
                        try:
                            # Look up user IDs from emails
                            user_ids = []
                            for email in followers:
                                try:
                                    user_id = get_user_id_by_email(server_url, auth_token, site_id, email)
                                    user_ids.append(user_id)
                                except ValueError as e:
                                    results.append({'success': False, 'message': f'  ⚠️  Could not find user: {email}'})
                            
                            # Add followers to the metric
                            if user_ids:
                                # Get existing followers to avoid duplicates
                                existing_followers = get_metric_followers(server_url, auth_token, new_metric_id)
                                new_followers = [uid for uid in user_ids if uid not in existing_followers]
                                
                                if new_followers:
                                    follower_result = batch_create_subscriptions(server_url, auth_token, new_metric_id, new_followers)
                                    if follower_result['success']:
                                        results.append({'success': True, 'message': f'  ✅ Added {len(new_followers)} follower(s)'})
                                        followers_added_count += len(new_followers)
                                    else:
                                        results.append({'success': False, 'message': f'  ❌ Failed to add followers: {follower_result["message"]}'})
                                        followers_failed_count += 1
                                else:
                                    results.append({'success': True, 'message': f'  ℹ️  All users already follow this metric'})
                        except Exception as follower_error:
                            results.append({'success': False, 'message': f'  ❌ Error adding followers: {str(follower_error)}'})
                            followers_failed_count += 1
                else:
                    error_msg = create_result.get('error', 'Unknown error')
                    api_response = create_result.get('response', '')
                    full_error = f"{error_msg}"
                    if api_response:
                        full_error += f" | API Response: {api_response}"
                    results.append({'success': False, 'message': f'[{i}/{len(metric_definitions)}] ❌ Failed: {filter_desc} - {full_error}'})
                    failed_count += 1
                    print(f"Failed to create metric: {full_error}")
                    
            except Exception as e:
                tb = traceback.format_exc()
                print(f"Exception creating metric {i}: {tb}")
                results.append({'success': False, 'message': f'[{i}/{len(metric_definitions)}] ❌ Error: {str(e)}'})
                failed_count += 1
        
        # Summary
        results.append({'success': True, 'message': '\n📊 SUMMARY'})
        results.append({'success': True, 'message': '=' * 60})
        results.append({'success': True, 'message': f'✅ Metrics processed: {created_count}'})
        results.append({'success': True, 'message': f'❌ Metrics failed: {failed_count}'})
        results.append({'success': True, 'message': f'📊 Total attempted: {len(metric_definitions)}'})
        
        if followers_added_count > 0:
            results.append({'success': True, 'message': f'👥 Followers added: {followers_added_count}'})
        
        if created_count > 0:
            success_rate = (created_count / len(metric_definitions)) * 100
            results.append({'success': True, 'message': f'📈 Success rate: {success_rate:.1f}%'})
        
        summary = f"Processed {created_count} scoped metrics successfully"
        if failed_count > 0:
            summary += f", {failed_count} failed"
        if followers_added_count > 0:
            summary += f", {followers_added_count} followers added"
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary,
            'created_count': created_count,
            'failed_count': failed_count
        })
        
    except Exception as e:
        # Get full stack trace
        tb_str = traceback.format_exc()
        print(f"ERROR in bulk_create_scoped_metrics: {tb_str}")  # Log to console
        
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}',
            'traceback': tb_str,
            'error_type': type(e).__name__
        })

@app.route('/pulse-analytics', methods=['POST'])
def pulse_analytics():
    """Generate analytics about Pulse metrics, followers, definitions, and datasources"""
    try:
        data = request.json
        results = []
        
        # Extract form data
        server_url = data.get('server_url', '').rstrip('/')
        api_version = data.get('api_version', '3.26')
        site_content_url = data.get('site_content_url', '')
        auth_method = data.get('auth_method')
        
        # Authentication data
        username = data.get('username')
        password = data.get('password')
        pat_name = data.get('pat_name')
        pat_token = data.get('pat_token')
        
        # Validate required fields
        if not all([server_url, auth_method]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: server_url and auth_method are required'
            })
        
        results.append({'success': True, 'message': '🚀 Starting Pulse Analytics...'})
        
        # Authenticate
        results.append({'success': True, 'message': '🔐 Authenticating with Tableau Server...'})
        
        auth_result = authenticate_tableau_rest(
            server_url, api_version, site_content_url, auth_method,
            username, password, pat_name, pat_token
        )
        
        if not auth_result['success']:
            return jsonify({
                'success': False,
                'error': f"Authentication failed: {auth_result['error']}"
            })
        
        auth_token = auth_result['auth_token']
        site_id = auth_result.get('site_id', '')
        results.append({'success': True, 'message': '✅ Authentication successful!'})
        
        # Get all datasources for name mapping
        results.append({'success': True, 'message': '🗄️ Retrieving datasource names...'})
        datasources_result = get_all_datasources_rest(server_url, auth_token, site_id, api_version)
        
        datasource_id_to_name = {}
        if datasources_result['success']:
            datasource_id_to_name = datasources_result['datasources']
            results.append({'success': True, 'message': f'✅ Found {len(datasource_id_to_name)} datasources'})
        else:
            results.append({'success': True, 'message': f'⚠️  Could not retrieve datasource names: {datasources_result.get("error")}'})
        
        # Get all definitions
        results.append({'success': True, 'message': '📊 Retrieving all metric definitions...'})
        definitions_result = get_metric_definitions_rest(server_url, auth_token)
        
        if not definitions_result['success']:
            return jsonify({
                'success': False,
                'error': f"Failed to get definitions: {definitions_result['error']}"
            })
        
        definitions = definitions_result.get('definitions', [])
        results.append({'success': True, 'message': f'✅ Found {len(definitions)} metric definitions'})
        
        # Debug: log first definition structure
        if definitions:
            print(f"DEBUG: First definition structure: {json.dumps(definitions[0], indent=2)}")
        
        # Get all subscriptions
        results.append({'success': True, 'message': '👥 Retrieving all subscriptions...'})
        subscriptions_result = get_all_subscriptions_rest(server_url, auth_token)
        
        if not subscriptions_result['success']:
            return jsonify({
                'success': False,
                'error': f"Failed to get subscriptions: {subscriptions_result['error']}"
            })
        
        all_subscriptions = subscriptions_result.get('subscriptions', [])
        results.append({'success': True, 'message': f'✅ Found {len(all_subscriptions)} total subscriptions'})
        
        # Debug: log first subscription structure
        if all_subscriptions:
            print(f"DEBUG: First subscription structure: {json.dumps(all_subscriptions[0], indent=2)}")
        
        # Extract unique metric IDs from subscriptions
        results.append({'success': True, 'message': '📈 Extracting metric IDs from subscriptions...'})
        
        unique_metric_ids = set()
        for sub in all_subscriptions:
            metric_id = sub.get('metric_id')
            if metric_id:
                unique_metric_ids.add(metric_id)
        
        results.append({'success': True, 'message': f'✅ Found {len(unique_metric_ids)} unique metric IDs'})
        
        # Fetch details for each unique metric
        results.append({'success': True, 'message': '📊 Retrieving metric details...'})
        
        all_metrics = []
        definition_metrics_map = {}
        
        for i, metric_id in enumerate(unique_metric_ids, 1):
            metric_result = get_metric_details_rest(server_url, auth_token, metric_id)
            
            if metric_result['success']:
                metric = metric_result['metric']
                all_metrics.append(metric)
                
                # Map metric to its definition
                def_id = metric.get('definition_id')
                if def_id:
                    if def_id not in definition_metrics_map:
                        definition_metrics_map[def_id] = []
                    definition_metrics_map[def_id].append(metric)
                
                if i % 25 == 0 or i == len(unique_metric_ids):
                    results.append({'success': True, 'message': f'  Progress: {i}/{len(unique_metric_ids)} metrics retrieved...'})
        
        results.append({'success': True, 'message': f'✅ Retrieved {len(all_metrics)} metric details'})
        
        # Debug: log first metric structure
        if all_metrics:
            print(f"DEBUG: First metric structure: {json.dumps(all_metrics[0], indent=2)}")
            print(f"DEBUG: Sample metric has definition_id: {all_metrics[0].get('definition_id')}")
            
        # Also check if any metrics have definition_ids not in our definitions list
        metric_def_ids = set(m.get('definition_id') for m in all_metrics if m.get('definition_id'))
        definition_ids = set(d.get('id') for d in definitions if d.get('id'))
        missing_def_ids = metric_def_ids - definition_ids
        
        if missing_def_ids:
            print(f"DEBUG: WARNING - Found {len(missing_def_ids)} definition IDs in metrics that are not in definitions list:")
            print(f"DEBUG: Missing definition IDs: {list(missing_def_ids)[:5]}")  # Show first 5
        
        # Build analytics data structures
        results.append({'success': True, 'message': '🔍 Analyzing data...'})
        
        # Map metric_id to subscription count
        metric_follower_count = {}
        unique_followers = set()
        
        for sub in all_subscriptions:
            metric_id = sub.get('metric_id')
            user_id = sub.get('follower', {}).get('user_id')
            
            if metric_id:
                metric_follower_count[metric_id] = metric_follower_count.get(metric_id, 0) + 1
            
            if user_id:
                unique_followers.add(user_id)
        
        # Build metric details with follower counts and names
        metrics_with_followers = []
        definition_id_to_name = {d.get('id'): d.get('name', 'Unnamed') for d in definitions}
        
        print(f"DEBUG: Definition ID to name map has {len(definition_id_to_name)} entries")
        
        for metric in all_metrics:
            metric_id = metric.get('id')
            follower_count = metric_follower_count.get(metric_id, 0)
            definition_id = metric.get('definition_id')
            
            # Try to get definition name from our map first
            definition_name = definition_id_to_name.get(definition_id)
            
            # If not found, try to get it from the metric's metadata
            if not definition_name:
                # Metrics might have metadata with the definition name
                metric_metadata = metric.get('metadata', {})
                definition_name = metric_metadata.get('name') or metric_metadata.get('definition_name')
                
                if definition_name:
                    print(f"DEBUG: Found definition name '{definition_name}' in metric metadata for definition_id {definition_id}")
                else:
                    print(f"DEBUG: Could not find definition name for definition_id {definition_id}, metric_id {metric_id}")
                    definition_name = 'Unknown Definition'
            
            # Build metric name: Definition name + (Default) or (Scoped)
            is_default = metric.get('is_default', False)
            metric_name = definition_name
            if is_default:
                metric_name += " (Default)"
            else:
                # Check if it has filters to indicate it's scoped
                filters = metric.get('specification', {}).get('filters', [])
                if filters:
                    metric_name += " (Scoped)"
            
            metrics_with_followers.append({
                'id': metric_id,
                'name': metric_name,
                'definition_id': definition_id,
                'definition_name': definition_name,
                'follower_count': follower_count,
                'is_default': is_default
            })
        
        print(f"DEBUG: Total metrics with followers built: {len(metrics_with_followers)}")
        print(f"DEBUG: Metric follower count map size: {len(metric_follower_count)}")
        print(f"DEBUG: Sample follower counts: {list(metric_follower_count.items())[:5]}")
        
        # Sort metrics by follower count
        top_metrics = sorted(metrics_with_followers, key=lambda x: x['follower_count'], reverse=True)[:10]
        
        print(f"DEBUG: Top metrics count: {len(top_metrics)}")
        if top_metrics:
            print(f"DEBUG: Top metric sample: {top_metrics[0]}")
        
        # Build definition analytics
        definition_analytics = []
        datasource_usage = {}
        
        for definition in definitions:
            def_id = definition.get('id', 'Unknown')
            def_name = definition.get('name', 'Unnamed')
            
            # Get datasource ID - try different possible locations
            def_datasource_id = None
            
            # Try specification.datasource.id first (most common location)
            if 'specification' in definition and 'datasource' in definition['specification']:
                spec_ds = definition['specification']['datasource']
                if isinstance(spec_ds, dict):
                    def_datasource_id = spec_ds.get('id') or spec_ds.get('luid')
                else:
                    def_datasource_id = str(spec_ds)
            
            # Fall back to direct datasource field
            if not def_datasource_id and 'datasource' in definition and definition['datasource']:
                if isinstance(definition['datasource'], dict):
                    def_datasource_id = definition['datasource'].get('id') or definition['datasource'].get('luid')
                else:
                    def_datasource_id = str(definition['datasource'])
            
            # Last resort
            if not def_datasource_id:
                def_datasource_id = definition.get('datasource_id', 'Unknown')
            
            is_certified = definition.get('certified', False)
            
            # Count metrics and followers for this definition
            def_metrics = definition_metrics_map.get(def_id, [])
            total_followers = sum([metric_follower_count.get(m.get('id'), 0) for m in def_metrics])
            
            definition_analytics.append({
                'id': def_id,
                'name': def_name,
                'datasource_id': def_datasource_id,
                'is_certified': is_certified,
                'metric_count': len(def_metrics),
                'total_followers': total_followers
            })
            
            # Track datasource usage
            if def_datasource_id and def_datasource_id != 'Unknown':
                if def_datasource_id not in datasource_usage:
                    datasource_usage[def_datasource_id] = {
                        'definition_count': 0,
                        'metric_count': 0,
                        'follower_count': 0
                    }
                
                datasource_usage[def_datasource_id]['definition_count'] += 1
                datasource_usage[def_datasource_id]['metric_count'] += len(def_metrics)
                datasource_usage[def_datasource_id]['follower_count'] += total_followers
        
        print(f"DEBUG: Definition analytics count: {len(definition_analytics)}")
        if definition_analytics:
            print(f"DEBUG: Sample definition analytics: {definition_analytics[0]}")
        
        print(f"DEBUG: Datasource usage count: {len(datasource_usage)}")
        if datasource_usage:
            print(f"DEBUG: Sample datasource: {list(datasource_usage.items())[0]}")
        
        # Sort definitions by total followers
        top_definitions = sorted(definition_analytics, key=lambda x: x['total_followers'], reverse=True)[:10]
        
        # Sort datasources by usage and add names
        top_datasources = sorted(
            [{'id': ds_id, 'name': datasource_id_to_name.get(ds_id, ds_id), **stats} for ds_id, stats in datasource_usage.items()],
            key=lambda x: x['follower_count'],
            reverse=True
        )[:10]
        
        print(f"DEBUG: Top definitions count: {len(top_definitions)}")
        print(f"DEBUG: Top datasources count: {len(top_datasources)}")
        
        # Build summary
        results.append({'success': True, 'message': '✅ Analysis complete!'})
        
        analytics_data = {
            'summary': {
                'total_definitions': len(definitions),
                'total_metrics': len(all_metrics),
                'total_subscriptions': len(all_subscriptions),
                'unique_followers': len(unique_followers),
                'certified_definitions': sum(1 for d in definitions if d.get('certified', False)),
                'unique_datasources': len(datasource_usage)
            },
            'top_metrics': top_metrics,
            'top_definitions': top_definitions,
            'top_datasources': top_datasources,
            'definition_details': definition_analytics
        }
        
        return jsonify({
            'success': True,
            'results': results,
            'analytics': analytics_data,
            'summary': f"✅ Analytics generated successfully! Found {len(definitions)} definitions, {len(all_metrics)} metrics, {len(all_subscriptions)} subscriptions from {len(unique_followers)} unique users"
        })
        
    except Exception as e:
        # Get full stack trace
        tb_str = traceback.format_exc()
        print(f"ERROR in pulse_analytics: {tb_str}")
        
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}',
            'traceback': tb_str,
            'error_type': type(e).__name__
        })

@app.route('/tcm-activity-logs', methods=['POST'])
def tcm_activity_logs():
    """Fetch Tableau Cloud Manager activity logs for a site"""
    try:
        data = request.json
        results = []
        
        # Extract form data
        tcm_uri = data.get('tcm_uri', '').rstrip('/')
        pat_token = data.get('pat_token', '').strip()
        site_luid = data.get('site_luid', '').strip()
        
        # Tableau credentials for Pulse API lookups
        tableau_server = data.get('tableau_server', '').rstrip('/')
        tableau_site_id = data.get('tableau_site_id', '').strip()
        tableau_pat_name = data.get('tableau_pat_name', '').strip()
        tableau_pat_token = data.get('tableau_pat_token', '').strip()
        
        event_type = 'metric_subscription_change'  # Hardcoded filter
        
        # Hardcoded date range: October 1-14, 2025
        target_start_date = datetime(2025, 10, 1, 0, 0, 0)
        target_end_date = datetime(2025, 10, 14, 23, 59, 59)
        
        # Validate required fields
        if not all([tcm_uri, pat_token, site_luid, tableau_server, tableau_site_id, tableau_pat_name, tableau_pat_token]):
            return jsonify({
                'success': False,
                'error': 'Missing required fields: all TCM and Tableau credentials are required'
            })
        
        results.append({'success': True, 'message': f'🚀 Starting Tableau Cloud Manager activity log retrieval...'})
        results.append({'success': True, 'message': f'📊 Site LUID: {site_luid}'})
        results.append({'success': True, 'message': f'📅 Fetching logs: October 1-14, 2025'})
        results.append({'success': True, 'message': f'🔍 Event type filter: {event_type}'})
        
        # Step 1: Login to TCM
        results.append({'success': True, 'message': '🔐 Authenticating with Tableau Cloud Manager...'})
        
        login_result = tcm_login(tcm_uri, pat_token)
        
        if not login_result['success']:
            return jsonify({
                'success': False,
                'error': f"TCM authentication failed: {login_result['error']}",
                'response': login_result.get('response', '')
            })
        
        session_token = login_result['session_token']
        tenant_id = login_result['tenant_id']
        
        results.append({'success': True, 'message': f'✅ Authentication successful! Tenant ID: {tenant_id}'})
        
        # Step 2: Split into 7-day chunks (TCM API limit)
        # Chunk 1: Oct 1-7, 2025
        # Chunk 2: Oct 8-14, 2025
        date_ranges = [
            {
                'start': '2025-10-01T00:00:00',
                'end': '2025-10-07T23:59:59',
                'label': 'Oct 1-7, 2025'
            },
            {
                'start': '2025-10-08T00:00:00',
                'end': '2025-10-14T23:59:59',
                'label': 'Oct 8-14, 2025'
            }
        ]
        
        results.append({'success': True, 'message': f'📅 Split into {len(date_ranges)} date range(s) (7-day API limit)'})
        
        # Step 3: Fetch file paths from all date ranges
        all_file_paths = []
        
        for i, date_range in enumerate(date_ranges, 1):
            results.append({'success': True, 'message': f'📂 Range {i}/{len(date_ranges)}: {date_range["label"]}'})
            
            paths_result = tcm_get_activity_log_paths(
                tcm_uri, session_token, tenant_id, site_luid, 
                date_range['start'], date_range['end'], 
                event_type=event_type
            )
            
            if not paths_result['success']:
                results.append({'success': False, 'message': f'  ❌ Failed: {paths_result["error"]}'})
                continue
            
            range_file_paths = paths_result['file_paths']
            all_file_paths.extend(range_file_paths)
            results.append({'success': True, 'message': f'  ✅ Found {len(range_file_paths)} file(s)'})
        
        if not all_file_paths:
            results.append({'success': True, 'message': '⚠️  No metric_subscription_change logs found for the specified date range'})
            
            return jsonify({
                'success': True,
                'results': results,
                'summary': 'No metric_subscription_change logs found',
                'file_count': 0
            })
        
        results.append({'success': True, 'message': f'\n✅ Total files to download: {len(all_file_paths)}'})
        
        # Step 4: Get download URLs (POST request)
        results.append({'success': True, 'message': f'🔗 Getting download URLs for {len(all_file_paths)} file(s)...'})
        
        urls_result = tcm_get_download_urls(tcm_uri, session_token, tenant_id, site_luid, all_file_paths)
        
        if not urls_result['success']:
            return jsonify({
                'success': False,
                'error': f"Failed to get download URLs: {urls_result['error']}",
                'response': urls_result.get('response', '')
            })
        
        # The response has a 'files' array, each with its own 'url'
        response_data = urls_result['data']
        files_with_urls = response_data.get('files', [])
        
        if not files_with_urls:
            return jsonify({
                'success': False,
                'error': 'No files with download URLs found in response',
                'response': json.dumps(response_data)
            })
        
        results.append({'success': True, 'message': f'✅ Got download URLs for {len(files_with_urls)} file(s)'})
        
        # Step 5: Download each log file
        results.append({'success': True, 'message': f'⬇️  Downloading {len(files_with_urls)} log file(s)...'})
        
        all_logs = []
        downloaded_count = 0
        failed_count = 0
        
        for i, file_obj in enumerate(files_with_urls, 1):
            download_url = file_obj.get('url')
            file_path = file_obj.get('path', f'file_{i}')
            
            if not download_url:
                results.append({'success': False, 'message': f'  ⚠️  File {i}: No download URL'})
                failed_count += 1
                continue
            
            if i % 10 == 0:
                results.append({'success': True, 'message': f'  [{i}/{len(files_with_urls)}] Downloading...'})
            
            download_result = tcm_download_log_file(download_url, session_token)
            
            if download_result['success']:
                log_content = download_result['content']
                all_logs.append(f"\n{'='*80}\n")
                all_logs.append(f"LOG FILE: {file_path}\n")
                all_logs.append(f"{'='*80}\n")
                all_logs.append(log_content)
                all_logs.append("\n")
                
                downloaded_count += 1
            else:
                failed_count += 1
                if failed_count <= 5:  # Only show first 5 failures
                    results.append({'success': False, 'message': f'  ❌ File {i} failed: {download_result["error"]}'})
        
        # Combine all logs
        combined_logs = ''.join(all_logs)
        results.append({'success': True, 'message': f'✅ Downloaded {downloaded_count}/{len(files_with_urls)} log file(s)'})
        
        # Step 6: Analyze the logs to extract subscription data
        results.append({'success': True, 'message': '\n📊 Analyzing subscription changes...'})
        
        # Parse all logs to extract subscription events
        subscription_events = []
        for log_entry in all_logs:
            if not log_entry.strip() or log_entry.startswith('=') or log_entry.startswith('LOG FILE:'):
                continue
            
            # Each log line should be JSON
            try:
                lines = log_entry.strip().split('\n')
                for line in lines:
                    if line.strip() and not line.startswith('=') and not line.startswith('LOG FILE:'):
                        try:
                            event = json.loads(line.strip())
                            subscription_events.append(event)
                        except json.JSONDecodeError:
                            continue
            except Exception:
                continue
        
        results.append({'success': True, 'message': f'  Found {len(subscription_events)} subscription events'})
        
        # Extract unique user LUIDs and metric IDs
        user_luids = set()
        metric_ids = set()
        
        # Track subscriptions per user and followers per metric
        user_metrics = {}  # user_luid -> set of metric_ids they're following
        metric_followers = {}  # metric_id -> set of user_luids following it
        
        for event in subscription_events:
            actor_luid = event.get('actorLuid')
            metric_id = event.get('metricId')
            event_action = event.get('action', '').lower()
            
            if not actor_luid or not metric_id:
                continue
            
            user_luids.add(actor_luid)
            metric_ids.add(metric_id)
            
            # Initialize if needed
            if actor_luid not in user_metrics:
                user_metrics[actor_luid] = set()
            if metric_id not in metric_followers:
                metric_followers[metric_id] = set()
            
            # Track subscription state (add/remove)
            if 'subscribe' in event_action or 'add' in event_action:
                user_metrics[actor_luid].add(metric_id)
                metric_followers[metric_id].add(actor_luid)
            elif 'unsubscribe' in event_action or 'remove' in event_action:
                user_metrics[actor_luid].discard(metric_id)
                metric_followers[metric_id].discard(actor_luid)
        
        results.append({'success': True, 'message': f'  Unique users: {len(user_luids)}'})
        results.append({'success': True, 'message': f'  Unique metrics: {len(metric_ids)}'})
        
        # Step 7: Authenticate with Tableau and look up names
        results.append({'success': True, 'message': '\n🔍 Authenticating with Tableau for name lookups...'})
        
        # Authenticate with PAT
        api_version = '3.19'  # Use current API version
        tableau_auth = authenticate_tableau_rest(
            tableau_server, api_version, tableau_site_id, 'pat',
            pat_name=tableau_pat_name, pat_token=tableau_pat_token
        )
        
        if not tableau_auth['success']:
            results.append({'success': False, 'message': f'  ⚠️  Tableau auth failed: {tableau_auth["error"]}'})
            results.append({'success': True, 'message': '  ℹ️  Continuing with IDs only...'})
            user_name_map = {}
            metric_name_map = {}
        else:
            auth_token = tableau_auth['auth_token']
            site_id_returned = tableau_auth['site_id']
            
            results.append({'success': True, 'message': f'  ✅ Tableau authentication successful'})
            
            # Get all users to build LUID -> username map
            results.append({'success': True, 'message': '  📋 Fetching users...'})
            try:
                users_url = f"{tableau_server}/api/{api_version}/sites/{site_id_returned}/users?pageSize=1000"
                users_response = requests.get(users_url, headers={'X-Tableau-Auth': auth_token}, verify=True, timeout=30)
                
                user_name_map = {}
                if users_response.status_code == 200:
                    users_data = ET.fromstring(users_response.content)
                    for user in users_data.findall('.//t:user', {'t': 'http://tableau.com/api'}):
                        user_luid = user.get('id')
                        username = user.get('name')
                        if user_luid and username:
                            user_name_map[user_luid] = username
                    results.append({'success': True, 'message': f'  ✅ Found {len(user_name_map)} users'})
                else:
                    results.append({'success': False, 'message': f'  ⚠️  Failed to fetch users: {users_response.status_code}'})
                    user_name_map = {}
            except Exception as e:
                results.append({'success': False, 'message': f'  ⚠️  Error fetching users: {str(e)}'})
                user_name_map = {}
            
            # Get all metric definitions to build metric_id -> name map
            results.append({'success': True, 'message': '  📊 Fetching metric definitions...'})
            try:
                definitions_url = f"{tableau_server}/api/-/pulse/definitions?page_size=1000"
                definitions_response = requests.get(definitions_url, headers={'X-Tableau-Auth': auth_token}, verify=True, timeout=30)
                
                metric_name_map = {}
                if definitions_response.status_code == 200:
                    definitions_data = definitions_response.json()
                    definitions = definitions_data.get('metric_definitions', [])
                    
                    # Need to get all metrics for each definition
                    # For now, use definition names as metric names
                    for definition in definitions:
                        definition_id = definition.get('metadata', {}).get('id')
                        definition_name = definition.get('metadata', {}).get('name')
                        
                        # Get metrics for this definition
                        # Note: We're mapping metric IDs to their definition names
                        # (In reality, metrics under same definition share the name)
                        for metric_id in metric_ids:
                            # This is a simplified approach - we'd need to call get metric details
                            # for each unique metric_id to get its definition_id
                            pass
                    
                    # Alternative: Get metric details for each unique metric_id
                    for metric_id in metric_ids:
                        try:
                            metric_url = f"{tableau_server}/api/-/pulse/metrics/{metric_id}"
                            metric_response = requests.get(metric_url, headers={'X-Tableau-Auth': auth_token}, verify=True, timeout=10)
                            
                            if metric_response.status_code == 200:
                                metric_data = metric_response.json()
                                def_id = metric_data.get('definition_id') or metric_data.get('metadata', {}).get('definition_id')
                                
                                # Find the definition name
                                for definition in definitions:
                                    if definition.get('metadata', {}).get('id') == def_id:
                                        metric_name = definition.get('metadata', {}).get('name', 'Unknown')
                                        # Check if it's a scoped metric
                                        filters = metric_data.get('specification', {}).get('filters', [])
                                        if filters:
                                            metric_name += ' (Scoped)'
                                        metric_name_map[metric_id] = metric_name
                                        break
                        except Exception:
                            continue
                    
                    results.append({'success': True, 'message': f'  ✅ Mapped {len(metric_name_map)}/{len(metric_ids)} metrics'})
                else:
                    results.append({'success': False, 'message': f'  ⚠️  Failed to fetch definitions: {definitions_response.status_code}'})
                    metric_name_map = {}
            except Exception as e:
                results.append({'success': False, 'message': f'  ⚠️  Error fetching metrics: {str(e)}'})
                metric_name_map = {}
        
        # Step 8: Create enriched reports
        results.append({'success': True, 'message': '\n📊 Generating enriched reports...'})
        
        user_report = []
        for user_luid, metrics in sorted(user_metrics.items(), key=lambda x: -len(x[1])):
            username = user_name_map.get(user_luid, user_luid)
            user_report.append({
                'user_luid': user_luid,
                'username': username,
                'metrics_following': len(metrics),
                'metric_ids': list(metrics)
            })
        
        metric_report = []
        for metric_id, followers in sorted(metric_followers.items(), key=lambda x: -len(x[1])):
            metric_name = metric_name_map.get(metric_id, metric_id)
            metric_report.append({
                'metric_id': metric_id,
                'metric_name': metric_name,
                'follower_count': len(followers),
                'follower_luids': list(followers)
            })
        
        results.append({'success': True, 'message': f'  ✅ Generated user report ({len(user_report)} users)'})
        results.append({'success': True, 'message': f'  ✅ Generated metric report ({len(metric_report)} metrics)'})
        
        # Step 9: Save enriched analysis reports
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        
        # Save user report
        user_report_filename = f"tcm_user_subscriptions_Oct1-14-2025_{site_luid}_{timestamp}.csv"
        user_report_path = os.path.join(os.path.dirname(__file__), user_report_filename)
        
        try:
            with open(user_report_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['User LUID', 'Username', 'Metrics Following'])
                for row in user_report:
                    writer.writerow([
                        row['user_luid'],
                        row['username'],
                        row['metrics_following']
                    ])
            results.append({'success': True, 'message': f'💾 User report saved: {user_report_filename}'})
        except Exception as e:
            results.append({'success': False, 'message': f'❌ Failed to save user report: {str(e)}'})
        
        # Save metric report
        metric_report_filename = f"tcm_metric_followers_Oct1-14-2025_{site_luid}_{timestamp}.csv"
        metric_report_path = os.path.join(os.path.dirname(__file__), metric_report_filename)
        
        try:
            with open(metric_report_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(['Metric ID', 'Metric Name', 'Follower Count'])
                for row in metric_report:
                    writer.writerow([
                        row['metric_id'],
                        row['metric_name'],
                        row['follower_count']
                    ])
            results.append({'success': True, 'message': f'💾 Metric report saved: {metric_report_filename}'})
        except Exception as e:
            results.append({'success': False, 'message': f'❌ Failed to save metric report: {str(e)}'})
        
        # Step 10: Save combined logs to text file  
        output_filename = f"tcm_metric_subscription_logs_Oct1-14-2025_{site_luid}_{timestamp}.txt"
        output_path = os.path.join(os.path.dirname(__file__), output_filename)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(f"TABLEAU CLOUD MANAGER - METRIC SUBSCRIPTION CHANGE LOGS\n")
                f.write(f"Site LUID: {site_luid}\n")
                f.write(f"Date Range: October 1-14, 2025\n")
                f.write(f"Event type: {event_type}\n")
                f.write(f"Total Files Downloaded: {downloaded_count}\n")
                f.write(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
                f.write("="*80 + "\n\n")
                f.write(combined_logs)
            
            results.append({'success': True, 'message': f'💾 Saved to file: {output_filename}'})
            results.append({'success': True, 'message': f'📁 Full path: {output_path}'})
            print(f"\n✅ Metric subscription logs saved to: {output_path}\n")
        except Exception as e:
            results.append({'success': False, 'message': f'⚠️  Failed to save file: {str(e)}'})
            print(f"ERROR saving file: {str(e)}")
        
        # Summary
        results.append({'success': True, 'message': '\n📊 SUMMARY'})
        results.append({'success': True, 'message': '=' * 60})
        results.append({'success': True, 'message': f'✅ Successfully downloaded: {downloaded_count} file(s)'})
        if failed_count > 0:
            results.append({'success': True, 'message': f'❌ Failed: {failed_count} file(s)'})
        results.append({'success': True, 'message': f'📊 Total log size: {len(combined_logs):,} characters'})
        results.append({'success': True, 'message': f'📅 Date range: October 1-14, 2025'})
        results.append({'success': True, 'message': f'🔍 Event type: {event_type}'})
        results.append({'success': True, 'message': f'📊 CSV Reports:'})
        results.append({'success': True, 'message': f'   • User subscriptions: {user_report_filename}'})
        results.append({'success': True, 'message': f'   • Metric followers: {metric_report_filename}'})
        results.append({'success': True, 'message': f'   • Raw logs: {output_filename}'})
        
        summary = f"Downloaded {downloaded_count} logs, analyzed {len(subscription_events)} events, created 2 CSV reports"
        if failed_count > 0:
            summary += f", {failed_count} failed"
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary,
            'log_count': downloaded_count,
            'failed_count': failed_count,
            'output_file': output_filename,
            'user_report_file': user_report_filename,
            'metric_report_file': metric_report_filename,
            'events_analyzed': len(subscription_events),
            'unique_users': len(user_luids),
            'unique_metrics': len(metric_ids)
        })
        
    except Exception as e:
        # Get full stack trace
        tb_str = traceback.format_exc()
        print(f"ERROR in tcm_activity_logs: {tb_str}")
        
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}',
            'traceback': tb_str,
            'error_type': type(e).__name__
        })

if __name__ == '__main__':
    # Run the Flask development server
    app.run(debug=True, host='0.0.0.0', port=3000)
