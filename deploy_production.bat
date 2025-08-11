@echo off
REM Production deployment script for STS Maintenance App (Windows)
REM This script sets up the production environment and starts the application

echo 🚀 Starting STS Maintenance App Production Deployment (Windows)
echo ==============================================================

REM Configuration
set "APP_DIR=%CD%"
set "PYTHON_CMD=python"
set "VENV_DIR=venv"
set "LOG_DIR=logs"
set "BACKUP_DIR=backups"

REM Function to log messages
set "timestamp=%date% %time%"

REM Check if Python is available
%PYTHON_CMD% --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    exit /b 1
)

echo ✅ Python found
%PYTHON_CMD% --version

REM Create virtual environment if it doesn't exist
if not exist "%VENV_DIR%" (
    echo 📦 Creating virtual environment...
    %PYTHON_CMD% -m venv %VENV_DIR%
)

REM Activate virtual environment
echo 🔧 Activating virtual environment...
call %VENV_DIR%\Scripts\activate.bat

REM Upgrade pip
echo ⬆️ Upgrading pip...
python -m pip install --upgrade pip

REM Install dependencies
echo 📚 Installing dependencies...
pip install -r requirements.txt

REM Create necessary directories
echo 📁 Creating necessary directories...
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
if not exist "instance" mkdir "instance"
if not exist "uploads" mkdir "uploads"
if not exist "static\uploads" mkdir "static\uploads"

REM Generate production secret key if .env doesn't exist
if not exist ".env" (
    echo 🔐 Creating production environment configuration...
    python -c "import secrets; print('SECRET_KEY=' + secrets.token_hex(32))" > .env
    echo FLASK_ENV=production >> .env
    echo FLASK_DEBUG=False >> .env
    echo # Database Configuration >> .env
    echo DATABASE_URL=sqlite:///instance/production.db >> .env
    echo # File Upload Configuration >> .env
    echo UPLOAD_FOLDER=uploads >> .env
    echo MAX_CONTENT_LENGTH=536870912 >> .env
    echo # Security Settings >> .env
    echo SECURE_SSL_REDIRECT=False >> .env
    echo SECURE_HSTS_SECONDS=0 >> .env
    echo ✅ Environment configuration created
) else (
    echo ✅ Environment configuration already exists
)

REM Run database initialization
echo 🗄️ Initializing database...
python -c "from app import create_app; app = create_app(); app.app_context().push(); from models import db; db.create_all(); print('Database initialized successfully')"

REM Test the application
echo 🧪 Testing application startup...
python -c "from app import create_app; app = create_app(); print('✅ Application can be imported and configured successfully')"
if errorlevel 1 (
    echo ❌ Application startup test failed
    exit /b 1
)

echo.
echo 🎉 Production deployment completed successfully!
echo ==============================================
echo.
echo 📊 Deployment Summary:
echo   • Virtual environment: %VENV_DIR%
echo   • Log directory: %LOG_DIR%
echo   • Database directory: instance
echo   • Uploads directory: uploads
echo   • Environment file: .env
echo.
echo 🚀 To start the application:
echo   1. Manual start:
echo      %VENV_DIR%\Scripts\activate.bat
echo      gunicorn --bind 0.0.0.0:5000 --workers 4 --timeout 120 app:app
echo.
echo   2. Development mode:
echo      %VENV_DIR%\Scripts\activate.bat
echo      python app.py
echo.
echo 🌐 Application will be available at:
echo   • Local: http://localhost:5000
echo   • Network: http://[YOUR-IP]:5000
echo   • AI Dashboard: http://localhost:5000/ai-dashboard
echo.
echo 📋 Next steps:
echo   • Review and update .env file for your environment
echo   • Configure Windows Firewall rules if needed
echo   • Set up SSL/HTTPS for production use
echo   • Configure backup schedule
echo   • Consider using IIS or Windows Service for production

echo.
echo Press any key to exit...
pause >nul

exit /b 0
