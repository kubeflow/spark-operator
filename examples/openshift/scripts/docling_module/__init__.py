"""
Docling Module - Document Processing Package
"""

from .processor import (
    # Main classes
    DoclingPDFProcessor,
    DocumentProcessorInterface,
    DocumentProcessorFactory,
    
    # Data classes
    ProcessingResult,
    DocumentConfig,
    
    # Simple function API
    docling_process,
)

__version__ = "1.0.0"
__author__ = "Rishabh Singh"

__all__ = [
    # Classes
    "DoclingPDFProcessor",
    "DocumentProcessorInterface",
    "DocumentProcessorFactory",
    "ProcessingResult",
    "DocumentConfig",
    
    # Functions
    "docling_process",
]
