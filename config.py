import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key-change-in-production'
    
    # Database configuration
    DATABASE_URL = os.environ.get('DATABASE_URL') or 'sqlite:///Stock.db'
    SQLALCHEMY_DATABASE_URI = DATABASE_URL
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # SQLite specific optimizations
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_timeout': 30,
        'pool_recycle': 1800,
        'pool_pre_ping': True,
        'pool_size': 5,        # Allow multiple connections
        'max_overflow': 10,    # Allow overflow connections
        'connect_args': {
            'timeout': 30,
            'check_same_thread': False
        }
    }
    
    # File upload configuration
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER') or 'uploads'
    MAX_CONTENT_LENGTH = int(os.environ.get('MAX_CONTENT_LENGTH', 512 * 1024 * 1024))  # 512MB (increased from 256MB)
    
    # Supported file extensions
    EXCEL_EXTENSIONS = {'xlsx', 'xls', 'xlsm', 'xlsb'}  # Excel formats
    CSV_EXTENSIONS = {'csv'}  # CSV format
    TEXT_EXTENSIONS = {'txt', 'tsv'}  # Text formats
    PDF_EXTENSIONS = {'pdf'}  # PDF format
    
    # All allowed extensions combined
    ALLOWED_EXTENSIONS = EXCEL_EXTENSIONS | CSV_EXTENSIONS | TEXT_EXTENSIONS | PDF_EXTENSIONS
    
    # Application configuration
    DEBUG = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'

class DevelopmentConfig(Config):
    DEBUG = True

class ProductionConfig(Config):
    DEBUG = False

config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}
