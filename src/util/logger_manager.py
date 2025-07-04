import logging, logging.handlers
import os
from datetime import datetime
from pathlib import Path

def setup_log_config(level:str='DEBUG', file_path: str = 'logs'):
    '''
    Setup the logger configuration
    
    Params
    ------
    level: log level used by the logger
    file_path: log files directory

    Returns
    -------
    log_file
    '''
    fpath = Path(file_path)
    fpath.mkdir(exist_ok=True)

    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = fpath / f"{level}_{ts}.log"
    error_logs = fpath / f"error_{ts}.log"

    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear any existing handlers
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # File handler for all logs (with rotation)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(file_handler)
    
    # Error file handler (errors and above only)
    error_handler = logging.handlers.RotatingFileHandler(
        error_logs,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=3
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(detailed_formatter)
    root_logger.addHandler(error_handler)
    
    # Log startup message
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized - Level: {level}, Log file: {log_file}")
    
    return log_file