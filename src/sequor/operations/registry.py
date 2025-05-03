import importlib
import logging
import os
import pkgutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger("sequor.operations.registry")

def register_all_operations() -> None:
    """Automatically import all operations from the operations package"""
    operations_pkg = 'operations'
    # Get the directory containing the operations
    operations_dir = Path(__file__).parent
    
    # Iterate through all .py files in the operations directory
    for module_info in pkgutil.iter_modules([str(operations_dir)]):
        # Skip __init__.py and registry.py
        if module_info.name in ['__init__', 'registry']:
            continue
            
        try:
            # Import each module
            importlib.import_module(f'{operations_pkg}.{module_info.name}')
            logger.debug(f"Successfully imported operation module: {module_info.name}")
        except Exception as e:
            logger.error(f"Failed to import operation module {module_info.name}: {str(e)}")
            # Don't raise the exception - we want to continue loading other operations 