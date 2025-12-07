"""
DTSX Parser Package

A comprehensive Python library for parsing and analyzing SQL Server Integration
Services (SSIS) packages (.dtsx files).

Features:
- Complete package configuration extraction
- Control flow stages with execution order
- Data flow transformations and routing logic
- Error handling strategy analysis
- Database objects identification
- Data flow diagrams (Mermaid and ASCII)
- Critical thresholds and alerts table

Usage:
    from dtsx_parser import DtsxParser, ReportGenerator

    parser = DtsxParser('package.dtsx')
    package = parser.parse()

    report_gen = ReportGenerator(package)
    report = report_gen.generate_full_report('markdown')
"""

from .models import (
    DtsxPackage,
    PackageMetadata,
    ConnectionManager,
    Variable,
    Parameter,
    ControlFlowStage,
    DataFlowTask,
    DataFlowComponent,
    DataFlowPath,
    EventHandler,
    ErrorHandlingStrategy,
    DatabaseObject,
    Threshold,
    Alert,
    DataFlowDiagram
)

from .parser import DtsxParser
from .diagram_generator import DiagramGenerator
from .report_generator import ReportGenerator

__version__ = '1.0.0'
__author__ = 'DTSX Parser'

__all__ = [
    'DtsxParser',
    'DiagramGenerator',
    'ReportGenerator',
    'DtsxPackage',
    'PackageMetadata',
    'ConnectionManager',
    'Variable',
    'Parameter',
    'ControlFlowStage',
    'DataFlowTask',
    'DataFlowComponent',
    'DataFlowPath',
    'EventHandler',
    'ErrorHandlingStrategy',
    'DatabaseObject',
    'Threshold',
    'Alert',
    'DataFlowDiagram'
]
