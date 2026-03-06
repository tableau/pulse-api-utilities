#!/bin/bash
# Deploy script - Push to both GitHub and Heroku

set -e  # Exit on error

echo "📦 Pushing to GitHub..."
git push origin main
echo "✅ GitHub push successful"
echo ""

echo "🚀 Deploying to Heroku..."
git push heroku main
echo "✅ Heroku deployment successful"
echo ""
echo "🌐 App live at: https://pulse-api-utilities-d5bcc201b2c8.herokuapp.com/"
