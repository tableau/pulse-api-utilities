from flask import Flask, render_template, request, jsonify
import requests
import json
import xml.etree.ElementTree as ET
import re
from urllib.parse import urlparse
from typing import List, Dict, Optional

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
    endpoint = f"{server_url}/api/-/pulse/definitions"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(endpoint, headers=headers, verify=True)
        
        if response.status_code == 200:
            response_data = response.json()
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
            
            # Extract metadata
            metadata = definition.get('metadata', {})
            certifier_luid = certification.get('modified_by', '')
            
            definition_info = {
                'id': metadata.get('id', ''),
                'name': metadata.get('name', ''),
                'certified': is_certified,
                'certification_note': certification.get('note', ''),
                'certified_by': certification.get('modified_by', 'Unknown'),
                'certified_at': certification.get('modified_at', ''),
                'certified_by_luid': certifier_luid
            }
            
            definitions.append(definition_info)
        
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

if __name__ == '__main__':
    # Run the Flask development server
    app.run(debug=True, host='0.0.0.0', port=3000)
