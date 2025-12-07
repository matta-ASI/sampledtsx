"""
DTSX Package Parser

Parses SQL Server Integration Services (SSIS) packages (.dtsx files)
and extracts all components including control flow, data flow, connections,
variables, parameters, and error handling configuration.
"""

import xml.etree.ElementTree as ET
import re
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from .models import (
    DtsxPackage, PackageMetadata, Annotation, ConnectionManager,
    Variable, Parameter, ControlFlowStage, DataFlowTask, DataFlowComponent,
    DataFlowPath, Column, OutputColumn, ConditionalOutput, SqlTask,
    SendMailTask, PrecedenceConstraint, EventHandler, ErrorHandlingStrategy,
    DatabaseObject, Threshold, Alert, ParameterBinding, ResultBinding
)


class DtsxParser:
    """Parser for DTSX (SSIS) package files."""

    # XML Namespaces
    NAMESPACES = {
        'DTS': 'www.microsoft.com/SqlServer/Dts',
        'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask',
        'SendMailTask': 'www.microsoft.com/sqlserver/dts/tasks/sendmailtask'
    }

    def __init__(self, file_path: str):
        """Initialize parser with DTSX file path."""
        self.file_path = Path(file_path)
        self.tree = None
        self.root = None
        self.package = None

    def parse(self) -> DtsxPackage:
        """Parse the DTSX file and return a DtsxPackage object."""
        self.tree = ET.parse(self.file_path)
        self.root = self.tree.getroot()

        # Register namespaces for proper parsing
        for prefix, uri in self.NAMESPACES.items():
            ET.register_namespace(prefix, uri)

        # Parse all components
        metadata = self._parse_metadata()
        annotations = self._parse_annotations()
        connection_managers = self._parse_connection_managers()
        variables = self._parse_variables()
        parameters = self._parse_parameters()

        # Parse control flow and data flow
        control_flow_stages, precedence_constraints = self._parse_control_flow()
        data_flow_tasks = self._parse_data_flow_tasks()

        # Parse error handling
        error_handling = self._parse_error_handling()

        # Extract database objects from SQL statements
        database_objects = self._extract_database_objects()

        # Extract thresholds and alerts
        thresholds = self._extract_thresholds(variables)
        alerts = self._extract_alerts()

        self.package = DtsxPackage(
            metadata=metadata,
            annotations=annotations,
            connection_managers=connection_managers,
            variables=variables,
            parameters=parameters,
            control_flow_stages=control_flow_stages,
            data_flow_tasks=data_flow_tasks,
            error_handling=error_handling,
            database_objects=database_objects,
            thresholds=thresholds,
            alerts=alerts,
            raw_precedence_constraints=precedence_constraints
        )

        return self.package

    def _get_attr(self, elem: ET.Element, attr: str, default: Any = None) -> Any:
        """Get attribute value with namespace prefix handling."""
        # Try with DTS namespace
        value = elem.get(f'{{{self.NAMESPACES["DTS"]}}}{attr}')
        if value is not None:
            return value
        # Try without namespace
        value = elem.get(attr)
        if value is not None:
            return value
        # Try with DTS: prefix
        value = elem.get(f'DTS:{attr}')
        return value if value is not None else default

    def _parse_metadata(self) -> PackageMetadata:
        """Parse package metadata from root element."""
        return PackageMetadata(
            name=self._get_attr(self.root, 'ObjectName', 'Unknown'),
            dtsid=self._get_attr(self.root, 'DTSID', ''),
            creation_date=self._get_attr(self.root, 'CreationDate'),
            creator_name=self._get_attr(self.root, 'CreatorName'),
            creator_computer=self._get_attr(self.root, 'CreatorComputerName'),
            version_build=self._get_attr(self.root, 'VersionBuild'),
            version_guid=self._get_attr(self.root, 'VersionGUID'),
            package_format_version=self._find_property_value('PackageFormatVersion'),
            last_modified_version=self._get_attr(self.root, 'LastModifiedProductVersion'),
            locale_id=self._get_attr(self.root, 'LocaleID')
        )

    def _find_property_value(self, prop_name: str) -> Optional[str]:
        """Find a property value in the package."""
        for prop in self.root.iter():
            if 'Property' in prop.tag and self._get_attr(prop, 'Name') == prop_name:
                return prop.text
        return None

    def _parse_annotations(self) -> List[Annotation]:
        """Parse package annotations/documentation."""
        annotations = []

        for elem in self.root.iter():
            if 'Annotations' in elem.tag:
                for ann in elem:
                    if 'Annotation' in ann.tag:
                        text_elem = None
                        for child in ann:
                            if 'AnnotationText' in child.tag:
                                text_elem = child.text
                                break

                        annotations.append(Annotation(
                            ref_id=self._get_attr(ann, 'refId', ''),
                            description=self._get_attr(ann, 'Description'),
                            tag=self._get_attr(ann, 'Tag'),
                            text=text_elem,
                            creation_date=self._get_attr(ann, 'CreationDate')
                        ))

        return annotations

    def _parse_connection_managers(self) -> List[ConnectionManager]:
        """Parse connection manager definitions."""
        connections = []

        for elem in self.root.iter():
            if elem.tag.endswith('ConnectionManagers'):
                for conn in elem:
                    if 'ConnectionManager' in conn.tag:
                        conn_type = self._get_attr(conn, 'CreationName', 'Unknown')
                        conn_string = None
                        props = {}

                        # Parse ObjectData for connection string
                        for obj_data in conn.iter():
                            if 'ObjectData' in obj_data.tag:
                                for child in obj_data:
                                    if 'ConnectionManager' in child.tag:
                                        conn_string = self._get_attr(child, 'ConnectionString')
                                        # Get additional properties
                                        for attr in child.attrib:
                                            clean_attr = attr.split('}')[-1] if '}' in attr else attr
                                            if clean_attr not in ['ConnectionString']:
                                                props[clean_attr] = child.attrib[attr]
                                    elif 'SmtpConnectionManager' in child.tag:
                                        conn_string = self._get_attr(child, 'ConnectionString')

                        # Parse server and database from connection string
                        server, database, provider = self._parse_connection_string(conn_string)

                        connections.append(ConnectionManager(
                            name=self._get_attr(conn, 'ObjectName', 'Unknown'),
                            ref_id=self._get_attr(conn, 'refId', ''),
                            dtsid=self._get_attr(conn, 'DTSID', ''),
                            connection_type=conn_type,
                            connection_string=conn_string,
                            server=server,
                            database=database,
                            provider=provider,
                            properties=props
                        ))

        return connections

    def _parse_connection_string(self, conn_string: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Parse connection string to extract server, database, and provider."""
        if not conn_string:
            return None, None, None

        server = None
        database = None
        provider = None

        # Parse common connection string formats
        patterns = {
            'server': r'(?:Data Source|Server)=([^;]+)',
            'database': r'(?:Initial Catalog|Database)=([^;]+)',
            'provider': r'Provider=([^;]+)'
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, conn_string, re.IGNORECASE)
            if match:
                if key == 'server':
                    server = match.group(1)
                elif key == 'database':
                    database = match.group(1)
                elif key == 'provider':
                    provider = match.group(1)

        return server, database, provider

    def _parse_variables(self) -> List[Variable]:
        """Parse package variables."""
        variables = []

        for elem in self.root.iter():
            if elem.tag.endswith('Variables') and not elem.tag.endswith('PackageVariables'):
                for var in elem:
                    if 'Variable' in var.tag:
                        value = None
                        data_type = 8  # Default to string

                        for child in var:
                            if 'VariableValue' in child.tag:
                                data_type = int(self._get_attr(child, 'DataType', 8))
                                value = child.text
                                break

                        variables.append(Variable(
                            name=self._get_attr(var, 'ObjectName', 'Unknown'),
                            namespace=self._get_attr(var, 'Namespace', 'User'),
                            dtsid=self._get_attr(var, 'DTSID', ''),
                            data_type=data_type,
                            value=value,
                            expression=self._get_attr(var, 'Expression'),
                            read_only=self._get_attr(var, 'ReadOnly', 'False') == 'True'
                        ))

        return variables

    def _parse_parameters(self) -> List[Parameter]:
        """Parse package parameters."""
        parameters = []

        for elem in self.root.iter():
            if 'PackageParameters' in elem.tag:
                for param in elem:
                    if 'PackageParameter' in param.tag:
                        value = None
                        for child in param:
                            if 'Property' in child.tag and self._get_attr(child, 'Name') == 'ParameterValue':
                                value = child.text
                                break

                        parameters.append(Parameter(
                            name=self._get_attr(param, 'ObjectName', 'Unknown'),
                            dtsid=self._get_attr(param, 'DTSID', ''),
                            data_type=int(self._get_attr(param, 'DataType', 8)),
                            value=value,
                            sensitive=self._get_attr(param, 'Sensitive', '0') == '1',
                            required=self._get_attr(param, 'Required', '0') == '1'
                        ))

        return parameters

    def _parse_control_flow(self) -> Tuple[List[ControlFlowStage], List[PrecedenceConstraint]]:
        """Parse control flow executables and precedence constraints."""
        stages = []
        all_constraints = []
        stage_order = 0

        # Find the main Executables container
        for elem in self.root:
            if 'Executables' in elem.tag:
                for executable in elem:
                    if 'Executable' in executable.tag:
                        stage_order += 1
                        stage = self._parse_executable_as_stage(executable, stage_order)
                        stages.append(stage)

        # Parse main precedence constraints
        for elem in self.root:
            if 'PrecedenceConstraints' in elem.tag:
                for constraint in elem:
                    if 'PrecedenceConstraint' in constraint.tag:
                        pc = self._parse_precedence_constraint(constraint)
                        all_constraints.append(pc)

        # Update stages with precedence information
        self._link_stages_with_precedence(stages, all_constraints)

        return stages, all_constraints

    def _parse_executable_as_stage(self, elem: ET.Element, order: int) -> ControlFlowStage:
        """Parse an executable element as a control flow stage."""
        exec_type = self._get_attr(elem, 'ExecutableType', '')
        creation_name = self._get_attr(elem, 'CreationName', '')

        # Determine stage type
        if 'SEQUENCE' in exec_type or 'SEQUENCE' in creation_name:
            stage_type = 'Sequence'
        elif 'Pipeline' in exec_type or 'Pipeline' in creation_name:
            stage_type = 'DataFlow'
        elif 'ExecuteSQLTask' in exec_type or 'ExecuteSQLTask' in creation_name:
            stage_type = 'SqlTask'
        elif 'SendMailTask' in exec_type or 'SendMailTask' in creation_name:
            stage_type = 'SendMailTask'
        else:
            stage_type = exec_type or creation_name or 'Unknown'

        # Parse child tasks
        tasks = []
        for child in elem:
            if 'Executables' in child.tag:
                for task_elem in child:
                    if 'Executable' in task_elem.tag:
                        task_info = self._parse_task(task_elem)
                        tasks.append(task_info)

        return ControlFlowStage(
            order=order,
            name=self._get_attr(elem, 'ObjectName', f'Stage_{order}'),
            stage_type=stage_type,
            description=self._get_attr(elem, 'Description'),
            tasks=tasks
        )

    def _parse_task(self, elem: ET.Element) -> Dict[str, Any]:
        """Parse a task element and return its details."""
        exec_type = self._get_attr(elem, 'ExecutableType', '')
        creation_name = self._get_attr(elem, 'CreationName', '')

        task_info = {
            'name': self._get_attr(elem, 'ObjectName', 'Unknown'),
            'ref_id': self._get_attr(elem, 'refId', ''),
            'dtsid': self._get_attr(elem, 'DTSID', ''),
            'type': exec_type or creation_name,
            'description': self._get_attr(elem, 'Description'),
        }

        # Parse SQL task specifics
        if 'ExecuteSQLTask' in (exec_type + creation_name):
            sql_info = self._parse_sql_task_details(elem)
            task_info.update(sql_info)

        # Parse Send Mail task specifics
        if 'SendMailTask' in (exec_type + creation_name):
            mail_info = self._parse_sendmail_task_details(elem)
            task_info.update(mail_info)

        return task_info

    def _parse_sql_task_details(self, elem: ET.Element) -> Dict[str, Any]:
        """Parse SQL task specific details."""
        details = {
            'sql_statement': None,
            'connection': None,
            'result_set_type': None,
            'parameter_bindings': [],
            'result_bindings': []
        }

        for obj_data in elem.iter():
            if 'SqlTaskData' in obj_data.tag:
                # Get SQL statement
                details['sql_statement'] = obj_data.get(
                    f'{{{self.NAMESPACES["SQLTask"]}}}SqlStatementSource',
                    obj_data.get('SQLTask:SqlStatementSource')
                )
                details['connection'] = obj_data.get(
                    f'{{{self.NAMESPACES["SQLTask"]}}}Connection',
                    obj_data.get('SQLTask:Connection')
                )
                details['result_set_type'] = obj_data.get(
                    f'{{{self.NAMESPACES["SQLTask"]}}}ResultSetType',
                    obj_data.get('SQLTask:ResultSetType')
                )

                # Parse parameter bindings
                for binding in obj_data.iter():
                    if 'ParameterBinding' in binding.tag:
                        details['parameter_bindings'].append({
                            'parameter_name': binding.get(
                                f'{{{self.NAMESPACES["SQLTask"]}}}ParameterName',
                                binding.get('SQLTask:ParameterName')
                            ),
                            'variable_name': binding.get(
                                f'{{{self.NAMESPACES["SQLTask"]}}}DtsVariableName',
                                binding.get('SQLTask:DtsVariableName')
                            )
                        })
                    elif 'ResultBinding' in binding.tag:
                        details['result_bindings'].append({
                            'result_name': binding.get(
                                f'{{{self.NAMESPACES["SQLTask"]}}}ResultName',
                                binding.get('SQLTask:ResultName')
                            ),
                            'variable_name': binding.get(
                                f'{{{self.NAMESPACES["SQLTask"]}}}DtsVariableName',
                                binding.get('SQLTask:DtsVariableName')
                            )
                        })

        return details

    def _parse_sendmail_task_details(self, elem: ET.Element) -> Dict[str, Any]:
        """Parse Send Mail task specific details."""
        details = {
            'from_address': None,
            'to_address': None,
            'subject': None,
            'message_source': None,
            'smtp_connection': None
        }

        for obj_data in elem.iter():
            if 'SendMailTaskData' in obj_data.tag:
                ns_prefix = f'{{{self.NAMESPACES["SendMailTask"]}}}'
                details['from_address'] = obj_data.get(f'{ns_prefix}From', obj_data.get('SendMailTask:From'))
                details['to_address'] = obj_data.get(f'{ns_prefix}To', obj_data.get('SendMailTask:To'))
                details['subject'] = obj_data.get(f'{ns_prefix}Subject', obj_data.get('SendMailTask:Subject'))
                details['smtp_connection'] = obj_data.get(f'{ns_prefix}SMTPServer', obj_data.get('SendMailTask:SMTPServer'))

                # Get message source from child element
                for child in obj_data:
                    if 'MessageSource' in child.tag:
                        details['message_source'] = child.text

        return details

    def _parse_precedence_constraint(self, elem: ET.Element) -> PrecedenceConstraint:
        """Parse a precedence constraint element."""
        return PrecedenceConstraint(
            name=self._get_attr(elem, 'ObjectName', ''),
            ref_id=self._get_attr(elem, 'refId', ''),
            dtsid=self._get_attr(elem, 'DTSID', ''),
            from_ref=self._get_attr(elem, 'From', ''),
            to_ref=self._get_attr(elem, 'To', ''),
            value=int(self._get_attr(elem, 'Value', 0)),
            logical_and=self._get_attr(elem, 'LogicalAnd', 'True') == 'True',
            expression=self._get_attr(elem, 'Expression'),
            eval_op=int(self._get_attr(elem, 'EvalOp', 0)) if self._get_attr(elem, 'EvalOp') else None
        )

    def _link_stages_with_precedence(self, stages: List[ControlFlowStage],
                                      constraints: List[PrecedenceConstraint]) -> None:
        """Link stages with their precedence constraints."""
        # Create lookup by ref_id
        stage_by_ref = {}
        for stage in stages:
            # Match by name in ref_id
            for constraint in constraints:
                if stage.name in constraint.from_ref:
                    stage_by_ref[constraint.from_ref] = stage
                if stage.name in constraint.to_ref:
                    stage_by_ref[constraint.to_ref] = stage

        # Set precedence information
        for constraint in constraints:
            from_stage = stage_by_ref.get(constraint.from_ref)
            to_stage = stage_by_ref.get(constraint.to_ref)

            if from_stage and constraint.to_ref not in from_stage.precedence_to:
                from_stage.precedence_to.append(constraint.to_ref)
                if constraint.expression:
                    to_stage.condition = constraint.expression if to_stage else None

            if to_stage and constraint.from_ref not in to_stage.precedence_from:
                to_stage.precedence_from.append(constraint.from_ref)

    def _parse_data_flow_tasks(self) -> List[DataFlowTask]:
        """Parse all data flow tasks in the package."""
        data_flows = []

        for elem in self.root.iter():
            if 'Executable' in elem.tag:
                exec_type = self._get_attr(elem, 'ExecutableType', '')
                creation_name = self._get_attr(elem, 'CreationName', '')

                if 'Pipeline' in exec_type or 'Pipeline' in creation_name:
                    dft = self._parse_data_flow_task(elem)
                    data_flows.append(dft)

        return data_flows

    def _parse_data_flow_task(self, elem: ET.Element) -> DataFlowTask:
        """Parse a single data flow task."""
        components = []
        paths = []

        # Find pipeline components
        for obj_data in elem.iter():
            if obj_data.tag == 'pipeline' or 'ObjectData' in obj_data.tag:
                for pipeline in obj_data.iter():
                    if pipeline.tag == 'pipeline':
                        # Parse components
                        for comp_container in pipeline:
                            if comp_container.tag == 'components':
                                for comp in comp_container:
                                    if comp.tag == 'component':
                                        component = self._parse_data_flow_component(comp)
                                        components.append(component)
                            elif comp_container.tag == 'paths':
                                for path in comp_container:
                                    if path.tag == 'path':
                                        df_path = self._parse_data_flow_path(path)
                                        paths.append(df_path)

        return DataFlowTask(
            name=self._get_attr(elem, 'ObjectName', 'Unknown'),
            ref_id=self._get_attr(elem, 'refId', ''),
            dtsid=self._get_attr(elem, 'DTSID', ''),
            description=self._get_attr(elem, 'Description'),
            components=components,
            paths=paths
        )

    def _parse_data_flow_component(self, elem: ET.Element) -> DataFlowComponent:
        """Parse a data flow component."""
        comp_class = elem.get('componentClassID', '')

        # Determine component type
        if 'Source' in comp_class:
            comp_type = 'Source'
        elif 'Destination' in comp_class:
            comp_type = 'Destination'
        else:
            comp_type = 'Transform'

        # Parse properties
        props = {}
        sql_command = None
        table_name = None

        for prop_container in elem:
            if prop_container.tag == 'properties':
                for prop in prop_container:
                    if prop.tag == 'property':
                        name = prop.get('name', '')
                        value = prop.text
                        props[name] = value

                        if name == 'SqlCommand':
                            sql_command = value
                        elif name == 'OpenRowset':
                            table_name = value

        # Parse output columns
        output_columns = []
        conditional_outputs = []
        has_error_output = False

        for outputs_container in elem:
            if outputs_container.tag == 'outputs':
                for output in outputs_container:
                    if output.tag == 'output':
                        is_error = output.get('isErrorOut', 'false').lower() == 'true'
                        if is_error:
                            has_error_output = True

                        output_name = output.get('name', '')

                        # Check for conditional split outputs
                        for out_props in output:
                            if out_props.tag == 'properties':
                                expr = None
                                friendly_expr = None
                                eval_order = None
                                is_default = False

                                for p in out_props:
                                    if p.tag == 'property':
                                        pname = p.get('name', '')
                                        if pname == 'Expression':
                                            expr = p.text
                                        elif pname == 'FriendlyExpression':
                                            friendly_expr = p.text
                                        elif pname == 'EvaluationOrder':
                                            eval_order = int(p.text) if p.text else None
                                        elif pname == 'IsDefaultOut':
                                            is_default = p.text and p.text.lower() == 'true'

                                if expr or is_default:
                                    conditional_outputs.append(ConditionalOutput(
                                        name=output_name,
                                        expression=expr,
                                        friendly_expression=friendly_expr,
                                        evaluation_order=eval_order,
                                        is_default=is_default
                                    ))

                        # Parse output columns
                        for out_cols in output:
                            if out_cols.tag == 'outputColumns':
                                for col in out_cols:
                                    if col.tag == 'outputColumn':
                                        output_columns.append(OutputColumn(
                                            name=col.get('name', ''),
                                            ref_id=col.get('refId', ''),
                                            data_type=col.get('dataType'),
                                            length=int(col.get('length', 0)) if col.get('length') else None,
                                            expression=col.get('expression')
                                        ))

        # Parse input columns
        input_columns = []
        for inputs_container in elem:
            if inputs_container.tag == 'inputs':
                for inp in inputs_container:
                    if inp.tag == 'input':
                        for inp_cols in inp:
                            if inp_cols.tag == 'inputColumns':
                                for col in inp_cols:
                                    if col.tag == 'inputColumn':
                                        input_columns.append(Column(
                                            name=col.get('cachedName', ''),
                                            ref_id=col.get('refId', ''),
                                            data_type=col.get('cachedDataType'),
                                            length=int(col.get('cachedLength', 0)) if col.get('cachedLength') else None
                                        ))

        # Get connection manager
        conn_manager = None
        for conns in elem:
            if conns.tag == 'connections':
                for conn in conns:
                    if conn.tag == 'connection':
                        conn_manager = conn.get('connectionManagerRefId', '')
                        break

        return DataFlowComponent(
            name=elem.get('name', 'Unknown'),
            ref_id=elem.get('refId', ''),
            component_type=comp_type,
            component_class=comp_class,
            description=elem.get('description'),
            input_columns=input_columns,
            output_columns=output_columns,
            conditional_outputs=conditional_outputs,
            connection_manager=conn_manager,
            sql_command=sql_command,
            table_name=table_name,
            properties=props,
            has_error_output=has_error_output
        )

    def _parse_data_flow_path(self, elem: ET.Element) -> DataFlowPath:
        """Parse a data flow path."""
        return DataFlowPath(
            name=elem.get('name', ''),
            ref_id=elem.get('refId', ''),
            source_ref_id=elem.get('startId', ''),
            destination_ref_id=elem.get('endId', '')
        )

    def _parse_error_handling(self) -> ErrorHandlingStrategy:
        """Parse error handling configuration."""
        event_handlers = []
        error_outputs = []
        logged_events = []
        logging_mode = None

        # Parse event handlers
        for elem in self.root.iter():
            if 'EventHandlers' in elem.tag:
                for handler in elem:
                    if 'EventHandler' in handler.tag:
                        eh = self._parse_event_handler(handler)
                        event_handlers.append(eh)

        # Parse logging configuration
        for elem in self.root.iter():
            if 'LoggingOptions' in elem.tag:
                for child in elem:
                    if 'LoggingMode' in child.tag:
                        logging_mode = child.text
                    elif 'EventFilter' in child.tag:
                        for event in child:
                            if 'EventToLog' in event.tag:
                                logged_events.append(event.text)

        return ErrorHandlingStrategy(
            event_handlers=event_handlers,
            error_outputs=error_outputs,
            logging_mode=logging_mode,
            logged_events=logged_events
        )

    def _parse_event_handler(self, elem: ET.Element) -> EventHandler:
        """Parse an event handler."""
        executables = []
        constraints = []

        for child in elem:
            if 'Executables' in child.tag:
                for exec_elem in child:
                    if 'Executable' in exec_elem.tag:
                        task = self._parse_task(exec_elem)
                        executables.append(task)
            elif 'PrecedenceConstraints' in child.tag:
                for pc_elem in child:
                    if 'PrecedenceConstraint' in pc_elem.tag:
                        pc = self._parse_precedence_constraint(pc_elem)
                        constraints.append(pc)

        return EventHandler(
            name=self._get_attr(elem, 'ObjectName', ''),
            ref_id=self._get_attr(elem, 'refId', ''),
            dtsid=self._get_attr(elem, 'DTSID', ''),
            event_name=self._get_attr(elem, 'EventName', ''),
            executables=executables,
            precedence_constraints=constraints
        )

    def _extract_database_objects(self) -> List[DatabaseObject]:
        """Extract database objects from SQL statements in the package."""
        objects = []
        seen = set()

        # Patterns for extracting database objects
        patterns = {
            'table': [
                r'FROM\s+(\[?[\w\.]+\]?\.\[?[\w]+\]?)',
                r'INTO\s+(\[?[\w\.]+\]?\.\[?[\w]+\]?)',
                r'UPDATE\s+(\[?[\w\.]+\]?\.\[?[\w]+\]?)',
                r'JOIN\s+(\[?[\w\.]+\]?\.\[?[\w]+\]?)',
                r'INSERT\s+INTO\s+(\[?[\w\.]+\]?\.\[?[\w]+\]?)',
            ],
            'procedure': [
                r'EXEC(?:UTE)?\s+(\[?[\w\.]+\]?\.\[?[\w]+\]?)',
            ],
            'function': [
                r'(\[?[\w\.]+\]?\.\[?fn_\w+\]?)\s*\(',
            ]
        }

        # Collect all SQL statements
        sql_statements = []

        for stage in self.package.control_flow_stages if self.package else []:
            for task in stage.tasks:
                if task.get('sql_statement'):
                    sql_statements.append((task.get('sql_statement'), task.get('name'), 'Task'))

        for dft in self.package.data_flow_tasks if self.package else []:
            for comp in dft.components:
                if comp.sql_command:
                    sql_statements.append((comp.sql_command, comp.name, comp.component_type))
                if comp.table_name:
                    # Direct table reference
                    name = comp.table_name.strip('[]')
                    if '.' in name:
                        parts = name.split('.')
                        schema = parts[0].strip('[]')
                        table = parts[1].strip('[]')
                    else:
                        schema = 'dbo'
                        table = name

                    key = f"{schema}.{table}"
                    if key not in seen:
                        seen.add(key)
                        objects.append(DatabaseObject(
                            name=table,
                            object_type='Table',
                            schema=schema,
                            usage=comp.component_type
                        ))

        # Parse SQL statements for objects
        for sql, source_name, usage in sql_statements:
            if not sql:
                continue

            # Extract tables
            for pattern in patterns['table']:
                matches = re.findall(pattern, sql, re.IGNORECASE)
                for match in matches:
                    name = match.strip('[]')
                    if '.' in name:
                        parts = name.split('.')
                        schema = parts[0].strip('[]')
                        table = parts[1].strip('[]')
                    else:
                        schema = 'dbo'
                        table = name

                    key = f"{schema}.{table}"
                    if key not in seen:
                        seen.add(key)
                        objects.append(DatabaseObject(
                            name=table,
                            object_type='Table',
                            schema=schema,
                            usage=usage
                        ))

            # Extract stored procedures
            for pattern in patterns['procedure']:
                matches = re.findall(pattern, sql, re.IGNORECASE)
                for match in matches:
                    name = match.strip('[]')
                    if '.' in name:
                        parts = name.split('.')
                        schema = parts[0].strip('[]')
                        proc = parts[1].strip('[]')
                    else:
                        schema = 'dbo'
                        proc = name

                    key = f"proc:{schema}.{proc}"
                    if key not in seen:
                        seen.add(key)
                        objects.append(DatabaseObject(
                            name=proc,
                            object_type='StoredProcedure',
                            schema=schema,
                            usage='Execute'
                        ))

            # Extract functions
            for pattern in patterns['function']:
                matches = re.findall(pattern, sql, re.IGNORECASE)
                for match in matches:
                    name = match.strip('[]')
                    if '.' in name:
                        parts = name.split('.')
                        schema = parts[0].strip('[]')
                        func = parts[1].strip('[]')
                    else:
                        schema = 'dbo'
                        func = name

                    key = f"func:{schema}.{func}"
                    if key not in seen:
                        seen.add(key)
                        objects.append(DatabaseObject(
                            name=func,
                            object_type='Function',
                            schema=schema,
                            usage='Reference'
                        ))

        return objects

    def _extract_thresholds(self, variables: List[Variable]) -> List[Threshold]:
        """Extract threshold values from variables."""
        thresholds = []

        # Known threshold variable patterns
        threshold_patterns = {
            'Threshold': 'General',
            'Limit': 'General',
            'Max': 'Performance',
            'Min': 'Performance',
            'Score': 'Fraud',
            'Window': 'Time',
            'Batch': 'Performance',
            'Fraud': 'Fraud',
            'Compliance': 'Compliance'
        }

        for var in variables:
            for pattern, category in threshold_patterns.items():
                if pattern.lower() in var.name.lower():
                    thresholds.append(Threshold(
                        name=var.name,
                        value=var.value,
                        data_type=str(var.data_type),
                        category=category,
                        description=f"Variable from namespace {var.namespace}"
                    ))
                    break

        return thresholds

    def _extract_alerts(self) -> List[Alert]:
        """Extract alert configurations from the package."""
        alerts = []

        # Look for send mail tasks and error handlers
        if self.package and self.package.error_handling:
            for handler in self.package.error_handling.event_handlers:
                for task in handler.executables:
                    if 'SendMailTask' in task.get('type', ''):
                        alerts.append(Alert(
                            name=task.get('name', 'Unknown Alert'),
                            alert_type=handler.event_name,
                            recipients=task.get('to_address'),
                            priority='High' if 'Error' in handler.event_name else 'Normal',
                            category='Error' if 'Error' in handler.event_name else 'Warning'
                        ))

        # Look for completion notifications
        for stage in self.package.control_flow_stages if self.package else []:
            for task in stage.tasks:
                if 'SendMailTask' in task.get('type', ''):
                    alerts.append(Alert(
                        name=task.get('name', 'Unknown Alert'),
                        alert_type='Completion',
                        recipients=task.get('to_address'),
                        priority='Normal',
                        category='Notification'
                    ))

        return alerts
