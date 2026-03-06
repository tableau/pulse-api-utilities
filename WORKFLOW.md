# Development & Deployment Workflow

## Quick Reference

### Working on a New Feature
```bash
# 1. Create and switch to feature branch
git checkout -b feature/your-feature-name

# 2. Make changes, test locally
python app.py  # Test at http://localhost:3000

# 3. Commit your changes
git add <files>
git commit -m "description of changes"

# 4. Push to GitHub (backup/review)
git push origin feature/your-feature-name
```

### Deploying to Production
```bash
# 1. Switch to main and merge your feature
git checkout main
git merge feature/your-feature-name

# 2. Deploy to both GitHub and Heroku (one command!)
./deploy.sh
```

## Deploy Script

A `deploy.sh` script is set up that pushes to both remotes with nice status messages.

## Full Workflow Example

```bash
# Start new feature
git checkout -b feature/add-new-utility
# ... make changes ...
git add app.py templates/index.html
git commit -m "feat: Add new utility for data export"
git push origin feature/add-new-utility

# When ready to deploy
git checkout main
git merge feature/add-new-utility
./deploy.sh  # ✅ Pushes to GitHub AND deploys to Heroku!

# Clean up feature branch (optional)
git branch -d feature/add-new-utility
git push origin --delete feature/add-new-utility
```

## Important Notes

- **Always work on feature branches** - keeps main clean
- **Test locally first** - run `python app.py` before deploying
- **GitHub secret scanning** - If push fails due to secrets, remove them and recommit
- **Heroku URL**: https://pulse-api-utilities-d5bcc201b2c8.herokuapp.com/

## Emergency: Push to Heroku Only

If GitHub is blocked but you need to deploy:
```bash
git push heroku main
```

## Checking Remote Status

```bash
git remote -v  # See all remotes
git log origin/main..main  # See commits not yet pushed to GitHub
git log heroku/main..main  # See commits not yet deployed to Heroku
```
