import logging
import os
from datetime import datetime

# ==============================
# 1. LOGGING CONFIGURATION
# ==============================

def setup_logger(log_dir="logs", log_file="ecommerce_data.log"):
    """Sets up a logger that writes to both console and a log file."""
    
    # Ensure the logs directory exists
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Define log file path
    log_file_path = os.path.join(log_dir, log_file)

    # Create logger
    logger = logging.getLogger("SparkPipelineLogger")
    logger.setLevel(logging.INFO)

    # Format for log messages
    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console handler (prints to terminal)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    # File handler (saves logs to a file)
    file_handler = logging.FileHandler(log_file_path, mode="a")
    file_handler.setFormatter(log_format)

    # Add handlers to logger
    if not logger.hasHandlers():
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

# Create a global logger instance
logger = setup_logger()
