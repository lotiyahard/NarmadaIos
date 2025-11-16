
# Railway Deployment Package

## Files Included
- **main.py** → Your service entry file (replace placeholder with your actual code)
- **Procfile** → Tells Railway to run your script as a worker
- **requirements.txt** → Python dependencies
- **.env.example** → Example environment variables

## Deploy Steps

1. Upload this folder to a GitHub repo.
2. Go to https://railway.app → New Project → Deploy from GitHub.
3. Add required environment variables in Railway dashboard (Variables tab).
4. Railway automatically installs dependencies and starts the worker:
   ```
   worker: python main.py
   ```

