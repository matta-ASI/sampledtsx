"""
Data models for DTSX package components.
These dataclasses represent all the extracted information from SSIS packages.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from enum import Enum
from datetime import datetime


class DataType(Enum):
    """SSIS data types mapped from DTS:DataType values."""
    INT16 = 2
    INT32 = 3
    SINGLE = 4
    DOUBLE = 5
    CURRENCY = 6
    DATETIME = 7
    STRING = 8
    BOOLEAN = 11
    OBJECT = 13
    DECIMAL = 14
    INT8 = 16
    UINT8 = 17
    UINT16 = 18
    UINT32 = 19
    INT64 = 20
    UINT64 = 21
    GUID = 72
    BYTES = 128
    WSTRING = 129
    NUMERIC = 131
    DBTIMESTAMP = 135


@dataclass
class PackageMetadata:
    """Package-level metadata and configuration."""
    name: str
    dtsid: str
    creation_date: Optional[str] = None
    creator_name: Optional[str] = None
    creator_computer: Optional[str] = None
    version_build: Optional[str] = None
    version_guid: Optional[str] = None
    package_format_version: Optional[str] = None
    last_modified_version: Optional[str] = None
    description: Optional[str] = None
    locale_id: Optional[str] = None


@dataclass
class Annotation:
    """Package annotation/documentation."""
    ref_id: str
    description: Optional[str] = None
    tag: Optional[str] = None
    text: Optional[str] = None
    creation_date: Optional[str] = None


@dataclass
class ConnectionManager:
    """Database/file connection configuration."""
    name: str
    ref_id: str
    dtsid: str
    connection_type: str  # OLEDB, FLATFILE, SMTP, etc.
    connection_string: Optional[str] = None
    server: Optional[str] = None
    database: Optional[str] = None
    provider: Optional[str] = None
    description: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Variable:
    """Package or container variable."""
    name: str
    namespace: str
    dtsid: str
    data_type: int
    value: Any
    expression: Optional[str] = None
    description: Optional[str] = None
    read_only: bool = False
    raise_event_on_change: bool = False


@dataclass
class Parameter:
    """Package parameter (configurable at runtime)."""
    name: str
    dtsid: str
    data_type: int
    value: Any
    description: Optional[str] = None
    sensitive: bool = False
    required: bool = False


@dataclass
class Column:
    """Data column definition."""
    name: str
    ref_id: Optional[str] = None
    data_type: Optional[str] = None
    length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_nullable: bool = True


@dataclass
class OutputColumn:
    """Data flow output column with expression."""
    name: str
    ref_id: Optional[str] = None
    data_type: Optional[str] = None
    length: Optional[int] = None
    expression: Optional[str] = None
    description: Optional[str] = None


@dataclass
class ConditionalOutput:
    """Conditional split output definition."""
    name: str
    expression: Optional[str] = None
    friendly_expression: Optional[str] = None
    evaluation_order: Optional[int] = None
    is_default: bool = False


@dataclass
class DataFlowComponent:
    """Component within a data flow task."""
    name: str
    ref_id: str
    component_type: str  # Source, Destination, Transform
    component_class: str  # Microsoft.OLEDBSource, etc.
    description: Optional[str] = None
    input_columns: List[Column] = field(default_factory=list)
    output_columns: List[OutputColumn] = field(default_factory=list)
    conditional_outputs: List[ConditionalOutput] = field(default_factory=list)
    connection_manager: Optional[str] = None
    sql_command: Optional[str] = None
    table_name: Optional[str] = None
    properties: Dict[str, Any] = field(default_factory=dict)
    has_error_output: bool = False


@dataclass
class DataFlowPath:
    """Connection between data flow components."""
    name: str
    ref_id: str
    source_ref_id: str
    destination_ref_id: str
    source_component: Optional[str] = None
    destination_component: Optional[str] = None


@dataclass
class DataFlowTask:
    """Data flow task containing transformation pipeline."""
    name: str
    ref_id: str
    dtsid: str
    description: Optional[str] = None
    components: List[DataFlowComponent] = field(default_factory=list)
    paths: List[DataFlowPath] = field(default_factory=list)


@dataclass
class ParameterBinding:
    """SQL task parameter binding."""
    parameter_name: str
    variable_name: str
    direction: str = "Input"
    data_type: Optional[int] = None


@dataclass
class ResultBinding:
    """SQL task result binding."""
    result_name: str
    variable_name: str


@dataclass
class SqlTask:
    """Execute SQL task configuration."""
    name: str
    ref_id: str
    dtsid: str
    description: Optional[str] = None
    connection_ref: Optional[str] = None
    sql_statement: Optional[str] = None
    result_set_type: Optional[str] = None
    parameter_bindings: List[ParameterBinding] = field(default_factory=list)
    result_bindings: List[ResultBinding] = field(default_factory=list)


@dataclass
class SendMailTask:
    """Send mail task configuration."""
    name: str
    ref_id: str
    dtsid: str
    description: Optional[str] = None
    smtp_connection: Optional[str] = None
    from_address: Optional[str] = None
    to_address: Optional[str] = None
    subject: Optional[str] = None
    message_source: Optional[str] = None
    priority: str = "Normal"


@dataclass
class PrecedenceConstraint:
    """Execution order constraint between tasks."""
    name: str
    ref_id: str
    dtsid: str
    from_ref: str
    to_ref: str
    value: int = 0  # 0=Success, 1=Failure, 2=Completion
    logical_and: bool = True
    expression: Optional[str] = None
    eval_op: Optional[int] = None  # 1=Expression, 2=Constraint, 3=Both


@dataclass
class Executable:
    """Base executable (task or container)."""
    name: str
    ref_id: str
    dtsid: str
    executable_type: str
    description: Optional[str] = None
    disabled: bool = False
    delay_validation: bool = False


@dataclass
class SequenceContainer(Executable):
    """Sequence container grouping tasks."""
    executables: List['Executable'] = field(default_factory=list)
    precedence_constraints: List[PrecedenceConstraint] = field(default_factory=list)


@dataclass
class ControlFlowStage:
    """A stage in the control flow with execution order."""
    order: int
    name: str
    stage_type: str  # Sequence, DataFlow, SqlTask, etc.
    description: Optional[str] = None
    tasks: List[Dict[str, Any]] = field(default_factory=list)
    precedence_from: List[str] = field(default_factory=list)
    precedence_to: List[str] = field(default_factory=list)
    condition: Optional[str] = None


@dataclass
class EventHandler:
    """Package event handler."""
    name: str
    ref_id: str
    dtsid: str
    event_name: str  # OnError, OnWarning, etc.
    executables: List[Dict[str, Any]] = field(default_factory=list)
    precedence_constraints: List[PrecedenceConstraint] = field(default_factory=list)


@dataclass
class ErrorHandlingStrategy:
    """Error handling configuration for the package."""
    event_handlers: List[EventHandler] = field(default_factory=list)
    error_outputs: List[Dict[str, Any]] = field(default_factory=list)
    fail_package_on_failure: bool = True
    max_error_count: int = 1
    logging_mode: Optional[str] = None
    logged_events: List[str] = field(default_factory=list)


@dataclass
class DatabaseObject:
    """Referenced database object."""
    name: str
    object_type: str  # Table, StoredProcedure, Function, View
    schema: str = "dbo"
    database: Optional[str] = None
    connection: Optional[str] = None
    usage: str = "Unknown"  # Source, Destination, Lookup, Reference


@dataclass
class Threshold:
    """Critical threshold or alert configuration."""
    name: str
    value: Any
    data_type: str
    description: Optional[str] = None
    category: str = "General"  # Fraud, Compliance, Performance, etc.
    action: Optional[str] = None


@dataclass
class Alert:
    """Alert configuration from the package."""
    name: str
    alert_type: str
    condition: Optional[str] = None
    recipients: Optional[str] = None
    priority: str = "Normal"
    category: str = "General"


@dataclass
class DataFlowDiagram:
    """Visual representation of a data flow."""
    name: str
    components: List[str] = field(default_factory=list)
    paths: List[tuple] = field(default_factory=list)
    mermaid_code: Optional[str] = None
    ascii_diagram: Optional[str] = None


@dataclass
class DtsxPackage:
    """Complete parsed DTSX package."""
    metadata: PackageMetadata
    annotations: List[Annotation] = field(default_factory=list)
    connection_managers: List[ConnectionManager] = field(default_factory=list)
    variables: List[Variable] = field(default_factory=list)
    parameters: List[Parameter] = field(default_factory=list)
    control_flow_stages: List[ControlFlowStage] = field(default_factory=list)
    data_flow_tasks: List[DataFlowTask] = field(default_factory=list)
    error_handling: Optional[ErrorHandlingStrategy] = None
    database_objects: List[DatabaseObject] = field(default_factory=list)
    thresholds: List[Threshold] = field(default_factory=list)
    alerts: List[Alert] = field(default_factory=list)
    data_flow_diagrams: List[DataFlowDiagram] = field(default_factory=list)
    raw_precedence_constraints: List[PrecedenceConstraint] = field(default_factory=list)
