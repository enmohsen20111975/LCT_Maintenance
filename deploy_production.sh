#!/bin/bash

# Production deployment script for STS Maintenance App
# This script sets up the production environment and starts the application

set -e  # Exit on any error

echo "🚀 Starting STS Maintenance App Production Deployment"
echo "=================================================="

# Configuration
APP_DIR="$(pwd)"
PYTHON_CMD="python3"
VENV_DIR="venv"
LOG_DIR="logs"
BACKUP_DIR="backups"

# Function to log messages
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Check if Python is available
if ! command -v $PYTHON_CMD &> /dev/null; then
    echo "❌ Python 3 is not installed or not in PATH"
    exit 1
fi

log "✅ Python found: $($PYTHON_CMD --version)"

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    log "📦 Creating virtual environment..."
    $PYTHON_CMD -m venv $VENV_DIR
fi

# Activate virtual environment
log "🔧 Activating virtual environment..."
source $VENV_DIR/bin/activate

# Upgrade pip
log "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install dependencies
log "📚 Installing dependencies..."
pip install -r requirements.txt

# Create necessary directories
log "📁 Creating necessary directories..."
mkdir -p $LOG_DIR
mkdir -p $BACKUP_DIR
mkdir -p instance
mkdir -p uploads
mkdir -p static/uploads

# Set proper permissions
chmod 755 instance uploads static/uploads $LOG_DIR $BACKUP_DIR

# Generate production secret key if .env doesn't exist
if [ ! -f ".env" ]; then
    log "🔐 Creating production environment configuration..."
    SECRET_KEY=$(python -c 'import secrets; print(secrets.token_hex(32))')
    cat > .env << EOF
# Production Environment Configuration
SECRET_KEY=${SECRET_KEY}
FLASK_ENV=production
FLASK_DEBUG=False

# Database Configuration
DATABASE_URL=sqlite:///instance/production.db

# File Upload Configuration
UPLOAD_FOLDER=uploads
MAX_CONTENT_LENGTH=536870912

# Security Settings
SECURE_SSL_REDIRECT=False
SECURE_HSTS_SECONDS=0
EOF
    log "✅ Environment configuration created"
else
    log "✅ Environment configuration already exists"
fi

# Run database initialization
log "🗄️ Initializing database..."
$PYTHON_CMD -c "
from app import create_app
app = create_app()
with app.app_context():
    from models import db
    db.create_all()
    print('Database initialized successfully')
"

# Test the application
log "🧪 Testing application startup..."
timeout 10s $PYTHON_CMD -c "
from app import create_app
app = create_app()
print('✅ Application can be imported and configured successfully')
" || {
    echo "❌ Application startup test failed"
    exit 1
}

# Create systemd service file (optional)
if command -v systemctl &> /dev/null; then
    log "⚙️ Creating systemd service file..."
    cat > sts-maintenance.service << EOF
[Unit]
Description=STS Maintenance App
After=network.target

[Service]
Type=exec
User=$USER
WorkingDirectory=$APP_DIR
Environment=PATH=$APP_DIR/$VENV_DIR/bin
ExecStart=$APP_DIR/$VENV_DIR/bin/gunicorn --bind 0.0.0.0:5000 --workers 4 --timeout 120 app:app
ExecReload=/bin/kill -s HUP \$MAINPID
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
    log "📝 Systemd service file created: sts-maintenance.service"
    log "   To install: sudo cp sts-maintenance.service /etc/systemd/system/"
    log "   To enable: sudo systemctl enable sts-maintenance"
    log "   To start: sudo systemctl start sts-maintenance"
fi

echo ""
echo "🎉 Production deployment completed successfully!"
echo "=================================================="
echo ""
echo "📊 Deployment Summary:"
echo "  • Virtual environment: $VENV_DIR"
echo "  • Log directory: $LOG_DIR"
echo "  • Database directory: instance"
echo "  • Uploads directory: uploads"
echo "  • Environment file: .env"
echo ""
echo "🚀 To start the application:"
echo "  1. Manual start:"
echo "     source $VENV_DIR/bin/activate"
echo "     gunicorn --bind 0.0.0.0:5000 --workers 4 --timeout 120 app:app"
echo ""
echo "  2. Development mode:"
echo "     source $VENV_DIR/bin/activate" 
echo "     python app.py"
echo ""
if command -v systemctl &> /dev/null; then
echo "  3. Production service (after installing systemd service):"
echo "     sudo systemctl start sts-maintenance"
echo ""
fi
echo "🌐 Application will be available at:"
echo "  • Local: http://localhost:5000"
echo "  • Network: http://$(hostname -I | awk '{print $1}'):5000"
echo "  • AI Dashboard: http://localhost:5000/ai-dashboard"
echo ""
echo "📋 Next steps:"
echo "  • Review and update .env file for your environment"
echo "  • Configure firewall rules if needed"
echo "  • Set up SSL/HTTPS for production use"
echo "  • Configure backup schedule"
echo ""

exit 0
