# Tableau Pulse Utilities

A comprehensive web application suite for managing Tableau Pulse with eight powerful utilities, built with Python Flask and featuring a modern, responsive UI.

## 🚀 Available Utilities

### 1. 📊 Copy Pulse Definitions
Transfer Tableau Pulse definitions between different sites
- Copy individual definitions by ID or all definitions from a datasource
- Cross-site copying with full authentication support
- Preserves all definition metadata and configurations

### 2. 👥 Bulk Manage Followers
Add or remove followers from Pulse metrics using email addresses
- Bulk operations on multiple metrics simultaneously
- Email-based user identification (converts emails to user IDs automatically)
- Support for both adding and removing followers

### 3. 🔄 Swap Datasources
Copy definitions with new datasources and migrate all metrics + followers
- Creates new definitions with different datasources
- Migrates all associated metrics and their followers
- Optional cleanup of old metric followers

### 4. ⚙️ Update User Preferences
Update Tableau Pulse user preferences for single or multiple users
- Configure cadence settings (daily, weekly, monthly)
- Manage email and Slack channel preferences
- Set metric grouping and sorting options
- Bulk update preferences for multiple users

### 5. ✅ Check Certified Metrics
View all certified metrics and optionally remove certifications from unauthorized users
- List all metric definitions with certification status
- Filter by group name (automatic group ID lookup)
- Automatically remove certifications from non-group members
- Comprehensive reporting with certification details

### 6. 📊 Bulk Create Scoped Metrics
Create multiple scoped metrics from a source metric with CSV upload support for advanced features
- **📄 CSV Upload Mode**: Upload a CSV file with dimension name, filter values, and follower emails
  - Column 1: Dimension name (e.g., "Region")
  - Column 2: Comma-separated filter values (e.g., "East, West" creates a metric with BOTH values)
  - Column 3: Comma-separated follower emails (optional)
- **✏️ Manual Entry Mode**: Simple text input for single filter value per metric
- **👥 Auto-assign Followers**: Automatically add followers by email when creating metrics
- **🔍 Multiple Filters**: Create metrics with multiple dimension values in a single filter
- Each new metric includes all filters from the source plus the new dimension filter(s)

### 7. 📈 Pulse Analytics
Get comprehensive insights and analytics about Pulse metric usage across your site
- **📊 Overall Statistics**: Total definitions, metrics, subscriptions, and unique followers
- **🏆 Top Metrics**: See which individual metrics have the most followers
- **📈 Top Definitions**: Discover which metric definitions are most popular across all their metrics
- **🗄️ Datasource Usage**: Understand which datasources are driving the most engagement
- **✅ Certification Insights**: Track how many definitions are certified
- **Interactive Dashboard**: Visual cards and sortable tables for easy analysis

### 8. 📑 Export Definitions
Export all Pulse metric definitions to CSV for documentation or analysis
- **📋 Basic Mode**: Export essential fields - Name, Measure, Time Dimension, Definitional Filters, Datasource Name
- **📊 Verbose Mode**: Export all configuration details including extension options, comparisons, certification status, and more
- **🎨 Viz State Support**: Handles Viz State definitions appropriately (marks measure/filters as embedded)
- **📥 Download CSV**: Tab-delimited CSV file with one row per definition
- **🗄️ Datasource Names**: Automatically resolves datasource IDs to names

## ✨ Common Features

- 🔐 **Multiple Authentication**: Supports both username/password and Personal Access Token (PAT) authentication
- 🌟 **Modern UI**: Beautiful, gradient interface with glass morphism effects and animations
- 🚀 **Real-time Progress**: Live updates during operations
- 📱 **Responsive Design**: Works seamlessly on desktop and mobile devices
- ✨ **Error Handling**: Comprehensive error reporting and validation
- 🏠 **Utility Selector**: Easy-to-use front page for choosing which tool to use

## Project Structure

```
hello-world-app/
├── app.py              # Flask application with Pulse API integration
├── requirements.txt    # Python dependencies (Flask, requests)
├── README.md          # This file
└── templates/
    └── index.html     # Web UI with form and progress tracking
```

## Setup and Installation

### Prerequisites

- Python 3.7+ installed on your system
- pip (Python package installer)

### Installation Steps

1. **Navigate to the project directory:**
   ```bash
   cd hello-world-app
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   ```

3. **Activate the virtual environment:**
   - On macOS/Linux:
     ```bash
     source venv/bin/activate
     ```
   - On Windows:
     ```bash
     venv\Scripts\activate
     ```

4. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Running the Application

1. **Start the Flask server:**
   ```bash
   python app.py
   ```

2. **Open your web browser and visit:**
   ```
   http://localhost:3000
   ```

3. **You'll see the Pulse Definition Copier interface with:**
   - Welcome tab with overview and features
   - Copy Definitions tab with the main form
   - Real-time progress tracking during operations

## 📋 Usage Guide

### Getting Started
1. Start the application and visit `http://localhost:3000`
2. Choose one of the three available utilities from the home page
3. Fill in the required connection and configuration details
4. Monitor real-time progress and results

### 📊 Using Copy Pulse Definitions
1. **Source Site Configuration**:
   - Enter source Tableau Server URL
   - Provide site content URL and authentication credentials
   - Specify the source datasource name

2. **Destination Site Configuration**:
   - Enter destination Tableau Server URL  
   - Provide destination site details and credentials
   - Specify the destination datasource name

3. **Definition Selection**:
   - Enter specific definition IDs (comma-separated) or "all" for all definitions
   
4. **Execute**: Click "🚀 Start Copying" and monitor progress

### 👥 Using Bulk Manage Followers
1. **Site Connection**:
   - Enter Tableau Server URL and site content URL
   - Choose authentication method and provide credentials

2. **Action Configuration**:
   - Select action: "Add Followers" or "Remove Followers"
   - Enter metric IDs (comma-separated)
   - Enter user email addresses (comma-separated or one per line)

3. **Execute**: Click "👥 Manage Followers" and monitor progress

### 🔄 Using Swap Datasources
1. **Site Connection**:
   - Enter Tableau Server URL and site content URL
   - Provide authentication credentials

2. **Copy Configuration**:
   - Enter the Pulse Definition ID to copy
   - Enter the new Datasource LUID to use

3. **Cleanup Options**:
   - Optionally remove followers from old metrics after copying

4. **Execute**: Click "🔄 Swap Datasources" and monitor progress

### ⚙️ Using Update User Preferences
1. **Server Connection**:
   - Enter Tableau Server URL and API version
   - Provide site content URL and authentication credentials

2. **Users Configuration**:
   - Enter user email addresses (comma-separated or one per line)

3. **Pulse Preferences**:
   - Set cadence (daily, weekly, monthly)
   - Configure email and Slack channel delivery
   - Set metric grouping and sort order preferences

4. **Execute**: Click "⚙️ Update User Preferences" and monitor progress

### ✅ Using Check Certified Metrics
1. **Server Connection**:
   - Enter Tableau Server URL and API version
   - Provide site content URL and authentication credentials

2. **Group Configuration (Optional)**:
   - Enter a Group Name to filter certifications by group membership
   - The tool will automatically look up the group ID from the name
   - Leave empty to view all certified metrics without filtering

3. **Certification Removal (Optional)**:
   - Check the box to automatically remove certifications from metrics certified by non-group members
   - Requires a Group Name to be specified

4. **Execute**: Click "✅ Check Certified Metrics" and view the results

   The tool will:
   - List all metric definitions with certification status
   - Show which metrics were certified by group members vs. non-members (if group specified)
   - Display certifier information and certification dates
   - Optionally remove certifications from unauthorized users

### 📊 Using Bulk Create Scoped Metrics
1. **Server Connection**:
   - Enter Tableau Server URL and API version
   - Provide site content URL and authentication credentials

2. **Source Metric Configuration**:
   - Enter the Source Metric ID (the existing scoped metric to use as a template)

3. **Input Mode Selection**:
   - **CSV Upload (Recommended)**:
     - Upload a CSV file with 3 columns:
       - Column 1: Dimension name (e.g., "Region")
       - Column 2: Comma-separated filter values (e.g., "East, West")
       - Column 3: Comma-separated follower emails (optional)
     - Example CSV:
       ```csv
       Region,East,user1@example.com
       Region,"East, West",user2@example.com
       Department,Sales,"user3@example.com, user4@example.com"
       ```
   - **Manual Entry**:
     - Enter Dimension Name (e.g., "Region", "Category")
     - Enter dimension values (comma-separated)
     - Creates one metric per value (no follower assignment)

4. **Execute**: Click "📊 Create Scoped Metrics" and monitor progress

   The tool will:
   - Retrieve the source metric and its specification
   - Create a new scoped metric for each CSV row (or dimension value in manual mode)
   - Each new metric will have all filters from the source metric plus new filter(s) for the dimension value(s)
   - Automatically add followers by email (CSV mode only)
   - Display success/failure for each created metric and follower addition

   **Example**: If you start with metric X that shows "Total Sales", choose dimension "Region", and provide values "East, West, North, South", the tool will create 4 new scoped metrics:
   - Total Sales (Region=East)
   - Total Sales (Region=West)
   - Total Sales (Region=North)
   - Total Sales (Region=South)

### 📈 Using Pulse Analytics
1. **Server Connection**:
   - Enter Tableau Server URL and API version
   - Provide site content URL and authentication credentials

2. **Generate Analytics**: Click "📈 Generate Analytics" and wait for data collection

   The tool will:
   - Retrieve all metric definitions on the site
   - Collect all subscriptions/followers data
   - Gather metrics for each definition
   - Analyze datasource usage patterns
   - Calculate statistics and rankings

3. **View Results**: The analytics dashboard displays:
   - **Summary Cards**: Quick overview of total definitions, metrics, subscriptions, unique followers, certified definitions, and unique datasources
   - **Top 10 Most Followed Metrics**: Individual metrics ranked by follower count
   - **Top 10 Definitions by Total Followers**: Metric definitions ranked by aggregate follower count across all their metrics
   - **Top 10 Datasources by Usage**: Datasources ranked by total followers, showing definition count and metric count

   **Use Cases**:
   - Understand which metrics and definitions are getting the most adoption
   - Identify underutilized datasources or definitions
   - Track certification progress across your organization
   - Make data-driven decisions about which metrics to promote or retire

### 📑 Using Export Definitions
1. **Server Connection**:
   - Enter Tableau Server URL and API version
   - Provide site content URL and authentication credentials

2. **Export Mode Selection**:
   - **Basic Mode**: Exports essential fields only
     - Name, Measure, Time Dimension, Definitional Filters, Type, Definition ID, Datasource Name
   - **Verbose Mode**: Exports all configuration details including:
     - All basic fields plus Description, Datasource info, Running Total setting
     - Extension options (Allowed Dimensions, Granularities, Offset settings)
     - Representation options (Number Format, Sentiment Type, Units)
     - Insights settings, Comparisons, Certification status
     - Related Links count, Goals count, Created/Modified timestamps

3. **Execute**: Click "📑 Export Definitions" and wait for processing

4. **View & Download Results**:
   - Preview the first 50 definitions in an interactive table
   - Click "📥 Download CSV" to get the complete tab-delimited file
   - File is also automatically saved to the server directory

   **Note on Viz State Definitions**:
   - Definitions created from existing visualizations show "(Viz State)" for Measure, Time Dimension, and Filters
   - These values are embedded in the visualization and cannot be extracted separately
   - All other fields (verbose mode) are exported normally

## 🔌 API Endpoints

- `GET /` - Main utilities selection interface
- `GET /api/hello` - Test API connection endpoint
- `POST /copy-definitions` - Copy pulse definitions between sites
- `POST /manage-followers` - Bulk add/remove followers from metrics
- `POST /swap-datasources` - Copy definitions with new datasources
- `POST /update-preferences` - Update user preferences for Pulse
- `POST /check-certified-metrics` - Check certified metrics and remove unauthorized certifications
- `POST /bulk-create-scoped-metrics` - Create multiple scoped metrics from a source metric
- `POST /pulse-analytics` - Generate comprehensive analytics about Pulse usage
- `POST /export-definitions` - Export all metric definitions to CSV

## Customization

- **Modify the UI:** Edit `templates/index.html` to change the interface or styling
- **Update CSS:** Modify the styles in the `<style>` section for custom themes
- **Add new API endpoints:** Extend `app.py` with additional `@app.route()` functions
- **Change server configuration:** Modify the `port=3000` parameter in `app.run()` in `app.py`
- **Adjust API version:** Update the `API_VERSION` constant in `app.py` if needed

## Development

- The app runs in debug mode by default, so changes to Python files will automatically reload the server
- For production deployment, set `debug=False` in `app.run()`
- All sensitive credentials are handled securely and not stored permanently

## Stopping the Application

- Press `Ctrl+C` in the terminal where the server is running
- To deactivate the virtual environment: `deactivate`

## Troubleshooting

### Common Issues

- **Port already in use:** Change the port in `app.py` or kill the process using port 3000
- **Module not found:** Make sure you've activated the virtual environment and installed requirements
- **Permission errors:** Ensure you have proper permissions in the project directory

### API-Related Issues

- **Authentication failed:** Verify your credentials and ensure the server URLs are correct
- **Datasource not found:** Check that the datasource name exists on the specified site
- **Definition copy failed:** Ensure you have appropriate permissions on both source and destination sites
- **Network timeout:** Check network connectivity to the Tableau servers

### Security Notes

- Never commit credentials to version control
- Use environment variables for production deployments
- Ensure HTTPS is used for production Tableau Server connections
- Personal Access Tokens are recommended over username/password authentication

## 🎯 What's Included

This application suite includes eight powerful utilities for managing Tableau Pulse:

### Available Utilities:
1. **Pulse Definition Copier** - Copy definitions between sites
2. **Bulk Manage Followers** - Add/remove followers from metrics  
3. **Swap Datasources** - Copy definitions with new datasources
4. **Update User Preferences** - Update Pulse user preferences
5. **Check Certified Metrics** - View and manage metric certifications
6. **Bulk Create Scoped Metrics** - Create multiple scoped metrics with dimension filters
7. **Pulse Analytics** - Get comprehensive insights into metric usage and follower engagement
8. **Export Definitions** - Export all metric definitions to CSV for documentation

### Benefits of the Web Interface:
- **🌐 No CLI Required**: Everything runs through the web browser
- **👁️ Better UX**: Visual progress tracking and comprehensive error reporting
- **✅ Validation**: Form validation ensures all required fields are completed
- **🔒 Security**: Credentials are handled securely without persistent storage
- **🚀 Enhanced Functionality**: Same power as the original CLI scripts with improved usability
- **📱 Accessibility**: Works on any device with a web browser
- **🏠 Unified Interface**: All eight utilities in one convenient location

### Technical Features:
- Modern Flask web framework
- RESTful API design
- Real-time progress updates via JSON responses
- Comprehensive error handling and logging
- Beautiful, responsive UI with animations
- Support for both JSON and XML API authentication methods

Transform your Tableau Pulse management workflow with this powerful web application suite! 🚀📊👥🔄⚙️✅📊📈📑
