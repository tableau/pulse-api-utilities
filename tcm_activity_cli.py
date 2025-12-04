#!/usr/bin/env python3
"""
Tableau Cloud Manager Activity Logs - Command Line Interface

This script provides a terminal-based interface to the TCM Activity Logs utility.
It prompts for all required information and generates the same analysis as the web UI.
"""

import requests
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
from getpass import getpass
import sys

# Check for optional dependencies
try:
    from tableauhyperapi import HyperProcess, Connection, CreateMode, TableDefinition, SqlType, TableName, Inserter, Telemetry
    HYPER_AVAILABLE = True
except ImportError:
    HYPER_AVAILABLE = False
    print("⚠️  Warning: tableauhyperapi not installed. Hyper extract creation will be disabled.")
    print("   Install with: pip install tableauhyperapi\n")

try:
    import tableauserverclient as TSC
    TSC_AVAILABLE = True
except ImportError:
    TSC_AVAILABLE = False
    print("⚠️  Warning: tableauserverclient not installed. Publishing to Tableau Cloud will be disabled.")
    print("   Install with: pip install tableauserverclient\n")


def print_header(text):
    """Print a formatted header."""
    print(f"\n{'=' * 80}")
    print(f"  {text}")
    print(f"{'=' * 80}\n")


def print_step(step_num, total_steps, text):
    """Print a formatted step."""
    print(f"\n[{step_num}/{total_steps}] {text}")


def print_success(text):
    """Print a success message."""
    print(f"✅ {text}")


def print_error(text):
    """Print an error message."""
    print(f"❌ {text}")


def print_info(text):
    """Print an info message."""
    print(f"ℹ️  {text}")


def tcm_login(tcm_uri, pat_token):
    """Authenticate with TCM using PAT."""
    login_url = f"{tcm_uri}/api/v1/pat/login"
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    payload = {
        'token': pat_token
    }
    
    try:
        response = requests.post(login_url, headers=headers, json=payload, verify=True, timeout=30)
        
        if response.status_code == 200:
            response_data = response.json()
            session_token = response_data.get('sessionToken')
            tenant_id = response_data.get('tenantId')
            
            if session_token and tenant_id:
                return {'success': True, 'session_token': session_token, 'tenant_id': tenant_id}
            else:
                return {'success': False, 'error': 'Missing sessionToken or tenantId in response'}
        else:
            return {'success': False, 'error': f'Login failed. Status: {response.status_code}'}
            
    except Exception as e:
        return {'success': False, 'error': f'Login error: {str(e)}'}


def tcm_get_activity_log_paths(tcm_uri, session_token, tenant_id, site_id, start_time, end_time, page_token=None, event_type=None):
    """Fetch activity log file paths from TCM."""
    params = {
        'startTime': start_time,
        'endTime': end_time
    }
    
    if page_token:
        params['pageToken'] = page_token
    
    query_string = '&'.join([f"{k}={quote(str(v), safe='')}" for k, v in params.items() if k != 'pageToken'])
    if page_token:
        query_string += f"&pageToken={page_token}"
    
    logs_url = f"{tcm_uri}/api/v1/tenants/{tenant_id}/sites/{site_id}/activitylog?{query_string}"
    
    headers = {
        'x-tableau-session-token': session_token,
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(logs_url, headers=headers, verify=True, timeout=30)
        
        if response.status_code == 200:
            if not response.text or response.text.strip() == '':
                return {'success': True, 'files': [], 'nextPageToken': None}
            
            response_data = response.json()
            files = response_data.get('files', [])
            next_page_token = response_data.get('nextPageToken')
            
            # Filter by event type if specified
            if event_type and files:
                filtered_files = [f for f in files if f'/eventType={event_type}/' in f.get('path', '')]
                return {'success': True, 'files': filtered_files, 'nextPageToken': next_page_token}
            
            return {'success': True, 'files': files, 'nextPageToken': next_page_token}
        elif response.status_code == 403:
            return {'success': True, 'files': [], 'nextPageToken': None, 'warning': 'Access denied for this page'}
        else:
            return {'success': False, 'error': f'Request failed. Status: {response.status_code}'}
            
    except Exception as e:
        return {'success': False, 'error': f'Request error: {str(e)}'}


def tcm_get_download_urls(tcm_uri, session_token, tenant_id, site_id, file_paths):
    """Get download URLs for activity log files."""
    download_url = f"{tcm_uri}/api/v1/tenants/{tenant_id}/sites/{site_id}/activitylog"
    
    headers = {
        'x-tableau-session-token': session_token,
        'Content-Type': 'application/json'
    }
    
    payload = {
        'tenantId': tenant_id,
        'files': file_paths
    }
    
    try:
        response = requests.post(download_url, headers=headers, json=payload, verify=True, timeout=60)
        
        if response.status_code == 200:
            response_data = response.json()
            files = response_data.get('files', [])
            
            download_urls = []
            for file_obj in files:
                url = file_obj.get('url')
                if url:
                    download_urls.append(url)
            
            if download_urls:
                return {'success': True, 'urls': download_urls}
            else:
                return {'success': False, 'error': 'No download URLs found in response'}
        else:
            return {'success': False, 'error': f'Request failed. Status: {response.status_code}'}
            
    except Exception as e:
        return {'success': False, 'error': f'Request error: {str(e)}'}


def tcm_download_log_file(download_url):
    """Download a single activity log file."""
    try:
        response = requests.get(download_url, verify=True, timeout=60)
        
        if response.status_code == 200:
            return {'success': True, 'content': response.text}
        else:
            return {'success': False, 'error': f'Download failed. Status: {response.status_code}'}
            
    except Exception as e:
        return {'success': False, 'error': f'Download error: {str(e)}'}


def authenticate_tableau_rest(server_url, site_id, pat_name, pat_secret, api_version):
    """Authenticate with Tableau REST API using PAT."""
    signin_url = f"{server_url}/api/{api_version}/auth/signin"
    
    payload = {
        "credentials": {
            "personalAccessTokenName": pat_name,
            "personalAccessTokenSecret": pat_secret,
            "site": {
                "contentUrl": site_id
            }
        }
    }
    
    try:
        response = requests.post(signin_url, json=payload, verify=True, timeout=30)
        
        if response.status_code == 200:
            response_data = response.json()
            token = response_data['credentials']['token']
            site_id_returned = response_data['credentials']['site']['id']
            
            return {'success': True, 'token': token, 'site_id': site_id_returned}
        else:
            return {'success': False, 'error': f'Authentication failed. Status: {response.status_code}'}
            
    except Exception as e:
        return {'success': False, 'error': f'Authentication error: {str(e)}'}


def get_all_users_on_site(server_url, api_version, site_id, auth_token):
    """Fetch all users on a Tableau site with pagination."""
    all_users = []
    page_number = 1
    page_size = 1000
    
    while True:
        users_url = f"{server_url}/api/{api_version}/sites/{site_id}/users?pageSize={page_size}&pageNumber={page_number}"
        
        headers = {
            'X-Tableau-Auth': auth_token,
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(users_url, headers=headers, verify=True, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                users = data.get('users', {}).get('user', [])
                
                if not users:
                    break
                
                all_users.extend(users)
                
                pagination = data.get('pagination', {})
                total_available = int(pagination.get('totalAvailable', 0))
                
                if page_number * page_size >= total_available:
                    break
                
                page_number += 1
            else:
                print(f"⚠️  Warning: Failed to fetch users page {page_number}")
                break
                
        except Exception as e:
            print(f"⚠️  Warning: Error fetching users: {str(e)}")
            break
    
    return all_users


def get_all_metric_definitions(server_url, auth_token):
    """Fetch all Pulse metric definitions with pagination."""
    all_definitions = []
    page_token = None
    page = 1
    
    while True:
        if page_token:
            url = f"{server_url}/api/-/pulse/definitions?page_size=1000&page_token={page_token}"
        else:
            url = f"{server_url}/api/-/pulse/definitions?page_size=1000"
        
        headers = {
            'X-Tableau-Auth': auth_token,
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, verify=True, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                definitions = []
                if 'definitions' in data:
                    definitions = data['definitions']
                elif 'metric_definitions' in data:
                    definitions = data['metric_definitions']
                elif 'data' in data:
                    definitions = data['data']
                
                if not definitions:
                    break
                
                all_definitions.extend(definitions)
                
                page_token = data.get('next_page_token')
                if not page_token:
                    break
                
                page += 1
            else:
                print(f"⚠️  Warning: Failed to fetch definitions page {page}")
                break
                
        except Exception as e:
            print(f"⚠️  Warning: Error fetching definitions: {str(e)}")
            break
    
    return all_definitions


def get_metric_details_rest(server_url, auth_token, metric_id):
    """Get details of a specific metric."""
    url = f"{server_url}/api/-/pulse/metrics/{metric_id}"
    
    headers = {
        'X-Tableau-Auth': auth_token,
        'Accept': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=True, timeout=30)
        
        if response.status_code == 200:
            metric_data = response.json().get('metric', {})
            return {'success': True, 'metric': metric_data}
        else:
            return {'success': False, 'error': f'Failed to get metric. Status: {response.status_code}'}
            
    except Exception as e:
        return {'success': False, 'error': str(e)}


def create_hyper_extract_from_data(data_rows, column_definitions, output_path, table_name='Extract'):
    """Create a Tableau Hyper extract from data."""
    if not HYPER_AVAILABLE:
        return {
            'success': False,
            'error': 'tableauhyperapi not installed. Run: pip install tableauhyperapi',
            'row_count': 0
        }
    
    try:
        with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, 'tcm-activity-cli') as hyper:
            with Connection(endpoint=hyper.endpoint,
                            create_mode=CreateMode.CREATE_AND_REPLACE,
                            database=output_path) as connection:
                connection.catalog.create_schema('Extract')
                
                columns = [TableDefinition.Column(display_name, sql_type) for display_name, sql_type, _ in column_definitions]
                schema = TableDefinition(
                    table_name=TableName('Extract', table_name),
                    columns=columns
                )
                connection.catalog.create_table(schema)
                
                with Inserter(connection, schema) as inserter:
                    for row in data_rows:
                        inserter.add_row([row.get(dict_key) for _, _, dict_key in column_definitions])
                    inserter.execute()
                
                row_count = connection.execute_scalar_query(f"SELECT COUNT(*) FROM {schema.table_name}")
        
        return {
            'success': True,
            'file_path': output_path,
            'row_count': row_count
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': f'Failed to create Hyper extract: {str(e)}',
            'row_count': 0
        }


def publish_hyper_file(server_url, site_id, auth_token, project_name, datasource_name, hyper_file_path, api_version):
    """Publish a Hyper file as a datasource to Tableau Cloud."""
    if not TSC_AVAILABLE:
        return {
            'success': False,
            'error': 'tableauserverclient not installed. Run: pip install tableauserverclient'
        }
    
    try:
        import xml.etree.ElementTree as ET
        
        # Get all projects with pagination
        all_projects = []
        page_number = 1
        page_size = 1000
        
        while True:
            projects_url = f"{server_url}/api/{api_version}/sites/{site_id}/projects?pageSize={page_size}&pageNumber={page_number}"
            
            projects_response = requests.get(
                projects_url,
                headers={'X-Tableau-Auth': auth_token},
                verify=True,
                timeout=30
            )
            
            if projects_response.status_code != 200:
                return {
                    'success': False,
                    'error': f'Failed to get projects: {projects_response.status_code}'
                }
            
            projects_data = ET.fromstring(projects_response.content)
            
            pagination = projects_data.find('.//t:pagination', {'t': 'http://tableau.com/api'})
            
            page_projects = []
            for project in projects_data.findall('.//t:project', {'t': 'http://tableau.com/api'}):
                proj_name = project.get('name')
                proj_id = project.get('id')
                
                project_info = {
                    'name': proj_name,
                    'id': proj_id
                }
                all_projects.append(project_info)
                page_projects.append(proj_name)
            
            if pagination is not None:
                total_available = int(pagination.get('totalAvailable', 0))
                page_size_returned = int(pagination.get('pageSize', page_size))
                
                if page_number * page_size_returned >= total_available:
                    break
            else:
                break
            
            page_number += 1
        
        # Find the project
        project_id = None
        project_name_lower = project_name.lower().strip()
        
        for project in all_projects:
            if project['name'] and project['name'].lower().strip() == project_name_lower:
                project_id = project['id']
                break
        
        if not project_id:
            all_project_names = [p['name'] for p in all_projects]
            return {
                'success': False,
                'error': f'Project "{project_name}" not found. Available projects: {", ".join(all_project_names[:20])}'
            }
        
        # Build multipart request
        publish_url = f"{server_url}/api/{api_version}/sites/{site_id}/datasources"
        
        xml_payload = f"""<?xml version='1.0' encoding='UTF-8'?>
<tsRequest>
    <datasource name='{datasource_name}'>
        <project id='{project_id}' />
    </datasource>
</tsRequest>"""
        
        with open(hyper_file_path, 'rb') as f:
            hyper_data = f.read()
        
        boundary = '----WebKitFormBoundary7MA4YWxkTrZu0gW'
        
        body_parts = []
        
        body_parts.append(f'--{boundary}'.encode())
        body_parts.append(b'Content-Disposition: form-data; name="request_payload"')
        body_parts.append(b'Content-Type: text/xml')
        body_parts.append(b'')
        body_parts.append(xml_payload.encode('utf-8'))
        
        body_parts.append(f'--{boundary}'.encode())
        body_parts.append(f'Content-Disposition: form-data; name="tableau_datasource"; filename="{os.path.basename(hyper_file_path)}"'.encode())
        body_parts.append(b'Content-Type: application/octet-stream')
        body_parts.append(b'')
        body_parts.append(hyper_data)
        
        body_parts.append(f'--{boundary}--'.encode())
        
        body = b'\r\n'.join(body_parts)
        
        headers = {
            'X-Tableau-Auth': auth_token,
            'Content-Type': f'multipart/form-data; boundary={boundary}'
        }
        
        response = requests.post(
            f"{publish_url}?overwrite=true",
            headers=headers,
            data=body,
            verify=True,
            timeout=120
        )
        
        if response.status_code in [200, 201]:
            response_data = ET.fromstring(response.content)
            datasource = response_data.find('.//t:datasource', {'t': 'http://tableau.com/api'})
            
            if datasource is not None:
                ds_id = datasource.get('id')
                ds_name = datasource.get('name')
                web_url = datasource.get('webpageUrl')
                
                return {
                    'success': True,
                    'datasource_id': ds_id,
                    'datasource_name': ds_name,
                    'web_url': web_url
                }
            else:
                return {'success': True, 'datasource_id': 'unknown'}
        else:
            return {
                'success': False,
                'error': f'Publish failed. Status: {response.status_code}'
            }
            
    except Exception as e:
        return {
            'success': False,
            'error': f'Publish error: {str(e)}'
        }


def main():
    """Main function to run the TCM Activity Logs CLI."""
    print_header("Tableau Cloud Manager - Activity Logs Analysis (CLI)")
    
    print("This tool retrieves activity logs from Tableau Cloud Manager,")
    print("analyzes metric subscription changes, and generates reports.\n")
    
    # Step 1: Get TCM credentials
    print_step(1, 10, "TCM Authentication")
    tcm_uri = input("TCM URI (e.g., https://xxx.cloudmanager.tableau.com): ").strip()
    tcm_pat = getpass("TCM Personal Access Token (hidden): ").strip()
    
    # Step 2: Get site LUID
    print_step(2, 10, "Tableau Site Information")
    site_luid = input("Site LUID: ").strip()
    
    # Step 3: Get date range
    print_step(3, 10, "Date Range Selection")
    print("1. Last 7 Days")
    print("2. Custom Date Range")
    date_choice = input("Select option (1 or 2): ").strip()
    
    if date_choice == "1":
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)
    else:
        start_date_str = input("Start Date (YYYY-MM-DD): ").strip()
        end_date_str = input("End Date (YYYY-MM-DD): ").strip()
        
        try:
            start_date = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        except ValueError:
            print_error("Invalid date format. Using last 7 days.")
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=7)
    
    print(f"\nDate range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Step 4: Get Tableau credentials for name lookups
    print_step(4, 10, "Tableau Cloud Authentication (for name lookups)")
    tableau_server = input("Tableau Server URL (e.g., https://10az.online.tableau.com): ").strip()
    site_id = input("Site ID (content URL, or empty for default): ").strip()
    pat_name = input("Personal Access Token Name: ").strip()
    pat_secret = getpass("Personal Access Token Secret (hidden): ").strip()
    api_version = input("API Version (e.g., 3.21): ").strip() or "3.21"
    
    # Step 5: Hyper extract options
    print_step(5, 10, "Hyper Extract Options")
    create_hyper = input("Create Tableau Hyper extracts? (y/n): ").strip().lower() == 'y'
    
    publish_datasources = False
    project_name = ""
    datasource_prefix = ""
    
    if create_hyper and HYPER_AVAILABLE:
        publish_datasources = input("Publish extracts to Tableau Cloud? (y/n): ").strip().lower() == 'y'
        
        if publish_datasources and TSC_AVAILABLE:
            project_name = input("Project Name: ").strip()
            datasource_prefix = input("Datasource Prefix (e.g., 'TCM Activity'): ").strip() or "TCM Activity"
    
    # Step 6: Authenticate with TCM
    print_step(6, 10, "Authenticating with TCM...")
    tcm_auth = tcm_login(tcm_uri, tcm_pat)
    
    if not tcm_auth['success']:
        print_error(f"TCM authentication failed: {tcm_auth['error']}")
        return 1
    
    session_token = tcm_auth['session_token']
    tenant_id = tcm_auth['tenant_id']
    print_success(f"Authenticated with TCM (Tenant: {tenant_id})")
    
    # Step 7: Fetch activity log file paths
    print_step(7, 10, "Fetching activity log file paths...")
    
    all_file_paths = []
    page_num = 1
    max_pages = 10
    
    # Split into 7-day chunks
    current_start = start_date
    chunk_num = 1
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=6, hours=23, minutes=59, seconds=59), end_date)
        
        print(f"\n  Chunk {chunk_num}: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
        
        start_time_str = current_start.strftime('%Y-%m-%dT%H:%M:%S')
        end_time_str = current_end.strftime('%Y-%m-%dT%H:%M:%S')
        
        page_token = None
        chunk_pages = 0
        
        while True:
            result = tcm_get_activity_log_paths(
                tcm_uri,
                session_token,
                tenant_id,
                site_luid,
                start_time_str,
                end_time_str,
                page_token=page_token,
                event_type='metric_subscription_change'
            )
            
            if not result['success']:
                print_error(f"Failed to fetch paths: {result['error']}")
                break
            
            files = result['files']
            all_file_paths.extend([f['path'] for f in files])
            
            chunk_pages += 1
            print(f"    Page {chunk_pages}: {len(files)} files")
            
            page_token = result.get('nextPageToken')
            
            if not page_token or chunk_pages >= max_pages:
                break
        
        current_start = current_end + timedelta(seconds=1)
        chunk_num += 1
    
    print_success(f"Found {len(all_file_paths)} activity log files")
    
    if not all_file_paths:
        print_error("No log files found for the specified date range.")
        return 1
    
    # Step 8: Download log files
    print_step(8, 10, "Downloading activity logs...")
    
    # Get download URLs
    print("  Requesting download URLs...")
    download_result = tcm_get_download_urls(tcm_uri, session_token, tenant_id, site_luid, all_file_paths)
    
    if not download_result['success']:
        print_error(f"Failed to get download URLs: {download_result['error']}")
        return 1
    
    download_urls = download_result['urls']
    print_success(f"Received {len(download_urls)} download URLs")
    
    # Download files
    combined_logs = []
    
    for idx, url in enumerate(download_urls, 1):
        print(f"  Downloading file {idx}/{len(download_urls)}...", end='\r')
        
        download = tcm_download_log_file(url)
        
        if download['success']:
            combined_logs.append(download['content'])
        else:
            print_error(f"Failed to download file {idx}: {download['error']}")
    
    print(f"\n")
    print_success(f"Downloaded {len(combined_logs)} log files")
    
    # Save combined logs to file
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    output_filename = f"tcm_activity_logs_{timestamp}.txt"
    output_path = os.path.join(os.getcwd(), output_filename)
    
    with open(output_path, 'w') as f:
        f.write(f"TCM Activity Logs\n")
        f.write(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC\n")
        f.write(f"Site LUID: {site_luid}\n")
        f.write(f"Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}\n")
        f.write(f"Total Files: {len(combined_logs)}\n")
        f.write(f"{'=' * 80}\n\n")
        
        for log in combined_logs:
            f.write(log)
            f.write("\n")
    
    print_success(f"Saved raw logs to: {output_filename}")
    
    # Step 9: Parse logs and analyze
    print_step(9, 10, "Analyzing logs...")
    
    user_subscriptions = {}  # user_luid -> count
    metric_followers = {}    # metric_id -> count
    
    for log_content in combined_logs:
        for line in log_content.split('\n'):
            if not line.strip():
                continue
            
            try:
                event = json.loads(line)
                
                if event.get('eventType') == 'metric_subscription_change':
                    actor_user_luid = event.get('actorUserLuid')
                    scoped_metric_id = event.get('scopedMetricId')
                    subscription_operation = event.get('subscriptionOperation')
                    
                    if actor_user_luid and scoped_metric_id and subscription_operation == 'FOLLOW_OPERATION_FOLLOW':
                        user_subscriptions[actor_user_luid] = user_subscriptions.get(actor_user_luid, 0) + 1
                        metric_followers[scoped_metric_id] = metric_followers.get(scoped_metric_id, 0) + 1
            
            except json.JSONDecodeError:
                continue
    
    print_success(f"Found {len(user_subscriptions)} users and {len(metric_followers)} metrics")
    
    # Step 10: Get names from Tableau
    print_step(10, 10, "Fetching names from Tableau Cloud...")
    
    # Authenticate with Tableau
    print("  Authenticating...")
    tableau_auth = authenticate_tableau_rest(tableau_server, site_id, pat_name, pat_secret, api_version)
    
    if not tableau_auth['success']:
        print_error(f"Tableau authentication failed: {tableau_auth['error']}")
        return 1
    
    auth_token = tableau_auth['token']
    site_id_returned = tableau_auth['site_id']
    print_success("Authenticated with Tableau Cloud")
    
    # Get all users
    print("  Fetching users...")
    all_users = get_all_users_on_site(tableau_server, api_version, site_id_returned, auth_token)
    user_name_map = {user['id']: user.get('name', user.get('email', 'Unknown')) for user in all_users}
    print_success(f"Fetched {len(user_name_map)} users")
    
    # Get all metric definitions
    print("  Fetching metric definitions...")
    all_definitions = get_all_metric_definitions(tableau_server, auth_token)
    definition_map = {}
    
    for definition in all_definitions:
        if 'metadata' in definition:
            def_id = definition['metadata'].get('id')
            def_name = definition['metadata'].get('name')
            if def_id and def_name:
                definition_map[def_id] = def_name
    
    print_success(f"Fetched {len(definition_map)} metric definitions")
    
    # Get metric names
    print("  Fetching metric details...")
    metric_name_map = {}
    
    for idx, metric_id in enumerate(metric_followers.keys(), 1):
        if idx % 10 == 0:
            print(f"    Processing metric {idx}/{len(metric_followers)}...", end='\r')
        
        metric_result = get_metric_details_rest(tableau_server, auth_token, metric_id)
        
        if metric_result['success']:
            metric_data = metric_result['metric']
            
            # Try to get metric name directly
            metric_name = metric_data.get('metadata', {}).get('name')
            
            if not metric_name:
                # Try to get definition name
                definition_id = metric_data.get('definition_id')
                if definition_id and definition_id in definition_map:
                    metric_name = definition_map[definition_id]
            
            if metric_name:
                metric_name_map[metric_id] = metric_name
    
    print(f"\n")
    print_success(f"Fetched {len(metric_name_map)} metric names")
    
    # Create reports
    user_report_data = []
    for user_luid, count in user_subscriptions.items():
        username = user_name_map.get(user_luid, f"Unknown ({user_luid})")
        user_report_data.append({
            'username': username,
            'metrics_following': count
        })
    
    user_report_data.sort(key=lambda x: x['metrics_following'], reverse=True)
    
    metric_report_data = []
    for metric_id, count in metric_followers.items():
        metric_name = metric_name_map.get(metric_id, f"Unknown ({metric_id})")
        metric_report_data.append({
            'metric_name': metric_name,
            'follower_count': count
        })
    
    metric_report_data.sort(key=lambda x: x['follower_count'], reverse=True)
    
    # Display results
    print_header("Analysis Results")
    
    print("\n📊 User Subscriptions (Top 10):")
    print(f"{'Username':<50} {'Metrics Following':>20}")
    print("-" * 72)
    for user in user_report_data[:10]:
        print(f"{user['username']:<50} {user['metrics_following']:>20}")
    
    print("\n📊 Metric Followers (Top 10):")
    print(f"{'Metric Name':<50} {'Follower Count':>20}")
    print("-" * 72)
    for metric in metric_report_data[:10]:
        print(f"{metric['metric_name']:<50} {metric['follower_count']:>20}")
    
    # Create Hyper extracts
    hyper_files = []
    
    if create_hyper and HYPER_AVAILABLE:
        print("\n💎 Creating Tableau Hyper extracts...")
        
        # User subscriptions extract
        user_hyper_filename = f"tcm_user_subscriptions_{timestamp}.hyper"
        user_hyper_path = os.path.join(os.getcwd(), user_hyper_filename)
        
        user_columns = [
            ('Username', SqlType.text(), 'username'),
            ('Metrics Following', SqlType.int(), 'metrics_following')
        ]
        
        user_hyper_result = create_hyper_extract_from_data(
            user_report_data,
            user_columns,
            user_hyper_path,
            'User_Subscriptions'
        )
        
        if user_hyper_result['success']:
            hyper_files.append(user_hyper_filename)
            print_success(f"Created: {user_hyper_filename} ({user_hyper_result['row_count']} rows)")
        else:
            print_error(f"Failed to create user extract: {user_hyper_result['error']}")
        
        # Metric followers extract
        metric_hyper_filename = f"tcm_metric_followers_{timestamp}.hyper"
        metric_hyper_path = os.path.join(os.getcwd(), metric_hyper_filename)
        
        metric_columns = [
            ('Metric Name', SqlType.text(), 'metric_name'),
            ('Follower Count', SqlType.int(), 'follower_count')
        ]
        
        metric_hyper_result = create_hyper_extract_from_data(
            metric_report_data,
            metric_columns,
            metric_hyper_path,
            'Metric_Followers'
        )
        
        if metric_hyper_result['success']:
            hyper_files.append(metric_hyper_filename)
            print_success(f"Created: {metric_hyper_filename} ({metric_hyper_result['row_count']} rows)")
        else:
            print_error(f"Failed to create metric extract: {metric_hyper_result['error']}")
    
    # Publish datasources
    if publish_datasources and TSC_AVAILABLE and hyper_files:
        print("\n📤 Publishing datasources to Tableau Cloud...")
        
        # Publish user subscriptions
        if user_hyper_result.get('success'):
            user_ds_name = f"{datasource_prefix} - User Subscriptions"
            print(f"  Publishing: {user_ds_name}...")
            
            publish_result = publish_hyper_file(
                tableau_server,
                site_id_returned,
                auth_token,
                project_name,
                user_ds_name,
                user_hyper_path,
                api_version
            )
            
            if publish_result['success']:
                print_success(f"Published: {user_ds_name}")
                if publish_result.get('web_url'):
                    print(f"         🔗 {publish_result['web_url']}")
            else:
                print_error(f"Failed: {publish_result['error']}")
        
        # Publish metric followers
        if metric_hyper_result.get('success'):
            metric_ds_name = f"{datasource_prefix} - Metric Followers"
            print(f"  Publishing: {metric_ds_name}...")
            
            publish_result = publish_hyper_file(
                tableau_server,
                site_id_returned,
                auth_token,
                project_name,
                metric_ds_name,
                metric_hyper_path,
                api_version
            )
            
            if publish_result['success']:
                print_success(f"Published: {metric_ds_name}")
                if publish_result.get('web_url'):
                    print(f"         🔗 {publish_result['web_url']}")
            else:
                print_error(f"Failed: {publish_result['error']}")
    
    # Summary
    print_header("Summary")
    print(f"✅ Raw logs saved: {output_filename}")
    print(f"✅ Users analyzed: {len(user_subscriptions)}")
    print(f"✅ Metrics analyzed: {len(metric_followers)}")
    
    if hyper_files:
        print(f"✅ Hyper extracts created: {len(hyper_files)}")
        for hyper_file in hyper_files:
            print(f"   - {hyper_file}")
    
    if publish_datasources and TSC_AVAILABLE:
        print(f"✅ Datasources published to project: {project_name}")
    
    print("\n🎉 Analysis complete!\n")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user. Exiting...")
        sys.exit(1)
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

