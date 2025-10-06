# Tableau Pulse Utilities

A comprehensive web application suite for managing Tableau Pulse with three powerful utilities, built with Python Flask and featuring a modern, responsive UI.

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

## 🔌 API Endpoints

- `GET /` - Main utilities selection interface
- `GET /api/hello` - Test API connection endpoint
- `POST /copy-definitions` - Copy pulse definitions between sites
- `POST /manage-followers` - Bulk add/remove followers from metrics
- `POST /swap-datasources` - Copy definitions with new datasources

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

This application suite converts three original command-line Python scripts into a unified, user-friendly web interface:

### Original Scripts Converted:
1. **Pulse Definition Copier** - Copy definitions between sites
2. **Bulk Manage Followers** - Add/remove followers from metrics  
3. **Swap Datasources** - Copy definitions with new datasources

### Benefits of the Web Interface:
- **🌐 No CLI Required**: Everything runs through the web browser
- **👁️ Better UX**: Visual progress tracking and comprehensive error reporting
- **✅ Validation**: Form validation ensures all required fields are completed
- **🔒 Security**: Credentials are handled securely without persistent storage
- **🚀 Enhanced Functionality**: Same power as the original CLI scripts with improved usability
- **📱 Accessibility**: Works on any device with a web browser
- **🏠 Unified Interface**: All three utilities in one convenient location

### Technical Features:
- Modern Flask web framework
- RESTful API design
- Real-time progress updates via JSON responses
- Comprehensive error handling and logging
- Beautiful, responsive UI with animations
- Support for both JSON and XML API authentication methods

Transform your Tableau Pulse management workflow with this powerful web application suite! 🚀📊👥🔄
