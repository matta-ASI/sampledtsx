"""
Report Generator for DTSX Packages

Generates comprehensive reports including:
- Package configuration
- Control flow stages with execution order
- Data flow transformations and routing logic
- Error handling strategy
- Database objects
- Data flow diagrams
- Critical thresholds and alerts
"""

import json
from typing import Optional
from datetime import datetime
from pathlib import Path

from .models import DtsxPackage
from .diagram_generator import DiagramGenerator


class ReportGenerator:
    """Generates comprehensive reports from parsed DTSX packages."""

    def __init__(self, package: DtsxPackage):
        """Initialize with a parsed DTSX package."""
        self.package = package
        self.diagram_gen = DiagramGenerator(package)

    def generate_full_report(self, output_format: str = 'text') -> str:
        """Generate a complete report in the specified format."""
        if output_format == 'json':
            return self._generate_json_report()
        elif output_format == 'markdown':
            return self._generate_markdown_report()
        else:
            return self._generate_text_report()

    def _generate_text_report(self) -> str:
        """Generate a plain text report."""
        sections = []

        sections.append(self._generate_header())
        sections.append(self._generate_package_configuration())
        sections.append(self._generate_control_flow_section())
        sections.append(self._generate_data_flow_section())
        sections.append(self._generate_error_handling_section())
        sections.append(self._generate_database_objects_section())
        sections.append(self._generate_diagrams_section())
        sections.append(self._generate_thresholds_alerts_section())

        return '\n\n'.join(sections)

    def _generate_markdown_report(self) -> str:
        """Generate a Markdown report."""
        lines = []

        # Header
        lines.append(f"# DTSX Package Analysis: {self.package.metadata.name}")
        lines.append(f"\n*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*\n")

        # Table of Contents
        lines.append("## Table of Contents\n")
        lines.append("1. [Package Configuration](#package-configuration)")
        lines.append("2. [Control Flow Stages](#control-flow-stages)")
        lines.append("3. [Data Flow Transformations](#data-flow-transformations)")
        lines.append("4. [Error Handling Strategy](#error-handling-strategy)")
        lines.append("5. [Database Objects](#database-objects)")
        lines.append("6. [Data Flow Diagrams](#data-flow-diagrams)")
        lines.append("7. [Critical Thresholds and Alerts](#critical-thresholds-and-alerts)")
        lines.append("")

        # Package Configuration
        lines.append("## Package Configuration\n")
        lines.append("### Metadata\n")
        lines.append(f"| Property | Value |")
        lines.append(f"|----------|-------|")
        lines.append(f"| Package Name | {self.package.metadata.name} |")
        lines.append(f"| DTSID | {self.package.metadata.dtsid} |")
        lines.append(f"| Creation Date | {self.package.metadata.creation_date or 'N/A'} |")
        lines.append(f"| Creator | {self.package.metadata.creator_name or 'N/A'} |")
        lines.append(f"| Version | {self.package.metadata.version_build or 'N/A'} |")
        lines.append(f"| Format Version | {self.package.metadata.package_format_version or 'N/A'} |")
        lines.append("")

        # Connection Managers
        lines.append("### Connection Managers\n")
        lines.append("| Name | Type | Server | Database |")
        lines.append("|------|------|--------|----------|")
        for conn in self.package.connection_managers:
            lines.append(f"| {conn.name} | {conn.connection_type} | {conn.server or 'N/A'} | {conn.database or 'N/A'} |")
        lines.append("")

        # Variables
        lines.append("### Package Variables\n")
        lines.append("| Name | Namespace | Data Type | Value |")
        lines.append("|------|-----------|-----------|-------|")
        for var in self.package.variables:
            value = str(var.value)[:50] if var.value else 'N/A'
            lines.append(f"| {var.name} | {var.namespace} | {var.data_type} | {value} |")
        lines.append("")

        # Parameters
        if self.package.parameters:
            lines.append("### Package Parameters\n")
            lines.append("| Name | Data Type | Value | Sensitive |")
            lines.append("|------|-----------|-------|-----------|")
            for param in self.package.parameters:
                lines.append(f"| {param.name} | {param.data_type} | {param.value or 'N/A'} | {'Yes' if param.sensitive else 'No'} |")
            lines.append("")

        # Control Flow
        lines.append("## Control Flow Stages\n")
        lines.append("### Execution Order\n")
        for stage in self.package.control_flow_stages:
            lines.append(f"#### Stage {stage.order}: {stage.name}\n")
            lines.append(f"- **Type:** {stage.stage_type}")
            if stage.description:
                lines.append(f"- **Description:** {stage.description}")
            if stage.condition:
                lines.append(f"- **Condition:** `{stage.condition}`")

            if stage.tasks:
                lines.append(f"\n**Tasks:**\n")
                for task in stage.tasks:
                    lines.append(f"- **{task.get('name', 'Unknown')}** ({task.get('type', 'Unknown')})")
                    if task.get('description'):
                        lines.append(f"  - {task.get('description')}")
                    if task.get('sql_statement'):
                        sql = task.get('sql_statement', '').strip()[:200]
                        lines.append(f"  - SQL: `{sql}...`")
            lines.append("")

        # Data Flow
        lines.append("## Data Flow Transformations\n")
        for dft in self.package.data_flow_tasks:
            lines.append(f"### {dft.name}\n")
            if dft.description:
                lines.append(f"*{dft.description}*\n")

            lines.append("#### Components\n")
            lines.append("| Component | Type | Class | Description |")
            lines.append("|-----------|------|-------|-------------|")
            for comp in dft.components:
                desc = comp.description or ''
                desc = desc[:50] + '...' if len(desc) > 50 else desc
                lines.append(f"| {comp.name} | {comp.component_type} | {comp.component_class.split('.')[-1]} | {desc} |")
            lines.append("")

            # Transformations detail
            lines.append("#### Transformation Details\n")
            for comp in dft.components:
                if comp.component_type == 'Transform':
                    lines.append(f"**{comp.name}** ({comp.component_class.split('.')[-1]})\n")

                    if comp.output_columns:
                        lines.append("Derived/Output Columns:\n")
                        for col in comp.output_columns:
                            if col.expression:
                                lines.append(f"- `{col.name}`: `{col.expression}`")
                            else:
                                lines.append(f"- `{col.name}`")
                        lines.append("")

                    if comp.conditional_outputs:
                        lines.append("Routing Conditions:\n")
                        for cond in comp.conditional_outputs:
                            if cond.is_default:
                                lines.append(f"- `{cond.name}`: Default (unmatched rows)")
                            else:
                                lines.append(f"- `{cond.name}`: `{cond.expression or cond.friendly_expression}`")
                        lines.append("")

            lines.append("")

        # Error Handling
        lines.append("## Error Handling Strategy\n")
        if self.package.error_handling:
            lines.append(f"**Logging Mode:** {self.package.error_handling.logging_mode or 'Default'}\n")

            if self.package.error_handling.logged_events:
                lines.append("**Logged Events:**\n")
                for event in self.package.error_handling.logged_events:
                    lines.append(f"- {event}")
                lines.append("")

            if self.package.error_handling.event_handlers:
                lines.append("### Event Handlers\n")
                for handler in self.package.error_handling.event_handlers:
                    lines.append(f"#### {handler.event_name}\n")
                    for task in handler.executables:
                        lines.append(f"- **{task.get('name', 'Unknown')}**: {task.get('description', 'N/A')}")
                    lines.append("")

        # Database Objects
        lines.append("## Database Objects\n")
        lines.append("### Tables\n")
        lines.append("| Schema | Name | Usage |")
        lines.append("|--------|------|-------|")
        for obj in self.package.database_objects:
            if obj.object_type == 'Table':
                lines.append(f"| {obj.schema} | {obj.name} | {obj.usage} |")
        lines.append("")

        sp_objects = [o for o in self.package.database_objects if o.object_type == 'StoredProcedure']
        if sp_objects:
            lines.append("### Stored Procedures\n")
            lines.append("| Schema | Name | Usage |")
            lines.append("|--------|------|-------|")
            for obj in sp_objects:
                lines.append(f"| {obj.schema} | {obj.name} | {obj.usage} |")
            lines.append("")

        func_objects = [o for o in self.package.database_objects if o.object_type == 'Function']
        if func_objects:
            lines.append("### Functions\n")
            lines.append("| Schema | Name | Usage |")
            lines.append("|--------|------|-------|")
            for obj in func_objects:
                lines.append(f"| {obj.schema} | {obj.name} | {obj.usage} |")
            lines.append("")

        # Diagrams
        lines.append("## Data Flow Diagrams\n")
        diagrams = self.diagram_gen.generate_all_diagrams()
        for diagram in diagrams:
            lines.append(f"### {diagram.name}\n")
            lines.append("```mermaid")
            lines.append(diagram.mermaid_code)
            lines.append("```\n")

            lines.append("<details>")
            lines.append("<summary>ASCII Diagram</summary>\n")
            lines.append("```")
            lines.append(diagram.ascii_diagram)
            lines.append("```")
            lines.append("</details>\n")

        # Execution Order
        lines.append("### Execution Order Diagram\n")
        lines.append("```")
        lines.append(self.diagram_gen.generate_execution_order_diagram())
        lines.append("```\n")

        # Routing Logic
        lines.append("### Data Routing Logic\n")
        lines.append("```")
        lines.append(self.diagram_gen.generate_routing_logic_diagram())
        lines.append("```\n")

        # Thresholds and Alerts
        lines.append("## Critical Thresholds and Alerts\n")
        lines.append("### Thresholds\n")
        lines.append("| Name | Value | Category | Data Type |")
        lines.append("|------|-------|----------|-----------|")
        for threshold in self.package.thresholds:
            lines.append(f"| {threshold.name} | {threshold.value} | {threshold.category} | {threshold.data_type} |")
        lines.append("")

        if self.package.alerts:
            lines.append("### Alerts\n")
            lines.append("| Name | Type | Category | Priority | Recipients |")
            lines.append("|------|------|----------|----------|------------|")
            for alert in self.package.alerts:
                recipients = alert.recipients[:30] + '...' if alert.recipients and len(alert.recipients) > 30 else (alert.recipients or 'N/A')
                lines.append(f"| {alert.name} | {alert.alert_type} | {alert.category} | {alert.priority} | {recipients} |")

        return '\n'.join(lines)

    def _generate_header(self) -> str:
        """Generate report header."""
        width = 80
        lines = []
        lines.append("=" * width)
        lines.append(" DTSX PACKAGE ANALYSIS REPORT ".center(width))
        lines.append("=" * width)
        lines.append(f" Package: {self.package.metadata.name}".ljust(width))
        lines.append(f" Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}".ljust(width))
        lines.append("=" * width)
        return '\n'.join(lines)

    def _generate_package_configuration(self) -> str:
        """Generate package configuration section."""
        lines = []
        lines.append("=" * 80)
        lines.append(" 1. PACKAGE CONFIGURATION")
        lines.append("=" * 80)

        # Metadata
        lines.append("\n1.1 METADATA")
        lines.append("-" * 40)
        lines.append(f"  Package Name:        {self.package.metadata.name}")
        lines.append(f"  DTSID:               {self.package.metadata.dtsid}")
        lines.append(f"  Creation Date:       {self.package.metadata.creation_date or 'N/A'}")
        lines.append(f"  Creator:             {self.package.metadata.creator_name or 'N/A'}")
        lines.append(f"  Creator Computer:    {self.package.metadata.creator_computer or 'N/A'}")
        lines.append(f"  Version Build:       {self.package.metadata.version_build or 'N/A'}")
        lines.append(f"  Package Format:      {self.package.metadata.package_format_version or 'N/A'}")
        lines.append(f"  Last Modified Ver:   {self.package.metadata.last_modified_version or 'N/A'}")

        # Connection Managers
        lines.append("\n1.2 CONNECTION MANAGERS")
        lines.append("-" * 40)
        for i, conn in enumerate(self.package.connection_managers, 1):
            lines.append(f"\n  [{i}] {conn.name}")
            lines.append(f"      Type:       {conn.connection_type}")
            lines.append(f"      Server:     {conn.server or 'N/A'}")
            lines.append(f"      Database:   {conn.database or 'N/A'}")
            lines.append(f"      Provider:   {conn.provider or 'N/A'}")

        # Variables
        lines.append("\n1.3 PACKAGE VARIABLES")
        lines.append("-" * 40)
        lines.append(f"  {'Name':<30} {'Namespace':<10} {'Type':<6} {'Value':<30}")
        lines.append(f"  {'-'*30} {'-'*10} {'-'*6} {'-'*30}")
        for var in self.package.variables:
            value = str(var.value)[:30] if var.value else 'N/A'
            lines.append(f"  {var.name:<30} {var.namespace:<10} {var.data_type:<6} {value:<30}")

        # Parameters
        if self.package.parameters:
            lines.append("\n1.4 PACKAGE PARAMETERS")
            lines.append("-" * 40)
            lines.append(f"  {'Name':<25} {'Type':<6} {'Sensitive':<10} {'Value':<30}")
            lines.append(f"  {'-'*25} {'-'*6} {'-'*10} {'-'*30}")
            for param in self.package.parameters:
                sens = "Yes" if param.sensitive else "No"
                value = str(param.value)[:30] if param.value else 'N/A'
                lines.append(f"  {param.name:<25} {param.data_type:<6} {sens:<10} {value:<30}")

        return '\n'.join(lines)

    def _generate_control_flow_section(self) -> str:
        """Generate control flow section."""
        lines = []
        lines.append("=" * 80)
        lines.append(" 2. CONTROL FLOW STAGES (EXECUTION ORDER)")
        lines.append("=" * 80)

        for stage in self.package.control_flow_stages:
            lines.append(f"\n{'='*60}")
            lines.append(f"STAGE {stage.order}: {stage.name}")
            lines.append(f"{'='*60}")
            lines.append(f"  Type:        {stage.stage_type}")
            if stage.description:
                lines.append(f"  Description: {stage.description}")
            if stage.condition:
                lines.append(f"  Condition:   {stage.condition}")

            if stage.precedence_from:
                lines.append(f"  Executes After: {', '.join(self._extract_names(stage.precedence_from))}")

            if stage.tasks:
                lines.append(f"\n  TASKS ({len(stage.tasks)} total):")
                lines.append(f"  {'-'*50}")
                for i, task in enumerate(stage.tasks, 1):
                    lines.append(f"\n    [{i}] {task.get('name', 'Unknown')}")
                    lines.append(f"        Type: {task.get('type', 'Unknown')}")
                    if task.get('description'):
                        lines.append(f"        Desc: {task.get('description')}")
                    if task.get('sql_statement'):
                        sql = task.get('sql_statement', '').strip()
                        sql_preview = sql[:100].replace('\n', ' ')
                        lines.append(f"        SQL:  {sql_preview}...")
                    if task.get('to_address'):
                        lines.append(f"        To:   {task.get('to_address')}")

        return '\n'.join(lines)

    def _generate_data_flow_section(self) -> str:
        """Generate data flow section."""
        lines = []
        lines.append("=" * 80)
        lines.append(" 3. DATA FLOW TRANSFORMATIONS AND ROUTING LOGIC")
        lines.append("=" * 80)

        for dft in self.package.data_flow_tasks:
            lines.append(f"\n{'='*70}")
            lines.append(f"DATA FLOW TASK: {dft.name}")
            lines.append(f"{'='*70}")
            if dft.description:
                lines.append(f"Description: {dft.description}")

            # Components summary
            sources = [c for c in dft.components if c.component_type == 'Source']
            transforms = [c for c in dft.components if c.component_type == 'Transform']
            destinations = [c for c in dft.components if c.component_type == 'Destination']

            lines.append(f"\nComponent Summary:")
            lines.append(f"  Sources:        {len(sources)}")
            lines.append(f"  Transforms:     {len(transforms)}")
            lines.append(f"  Destinations:   {len(destinations)}")

            # Sources
            if sources:
                lines.append(f"\n3.1 SOURCES")
                lines.append(f"{'-'*50}")
                for src in sources:
                    lines.append(f"\n  [{src.name}]")
                    lines.append(f"    Class: {src.component_class}")
                    if src.connection_manager:
                        lines.append(f"    Connection: {src.connection_manager}")
                    if src.sql_command:
                        sql = src.sql_command.strip()[:200].replace('\n', ' ')
                        lines.append(f"    SQL: {sql}...")

            # Transformations
            if transforms:
                lines.append(f"\n3.2 TRANSFORMATIONS")
                lines.append(f"{'-'*50}")
                for trn in transforms:
                    lines.append(f"\n  [{trn.name}]")
                    lines.append(f"    Class: {trn.component_class}")
                    if trn.description:
                        lines.append(f"    Description: {trn.description}")

                    # Output columns with expressions
                    if trn.output_columns:
                        lines.append(f"    Derived Columns:")
                        for col in trn.output_columns:
                            if col.expression:
                                expr = col.expression[:60]
                                lines.append(f"      - {col.name}: {expr}")

                    # Conditional outputs (routing)
                    if trn.conditional_outputs:
                        lines.append(f"    Routing Logic:")
                        for cond in trn.conditional_outputs:
                            if cond.is_default:
                                lines.append(f"      - {cond.name}: DEFAULT (unmatched rows)")
                            else:
                                expr = cond.friendly_expression or cond.expression
                                lines.append(f"      - {cond.name}: {expr}")

            # Destinations
            if destinations:
                lines.append(f"\n3.3 DESTINATIONS")
                lines.append(f"{'-'*50}")
                for dst in destinations:
                    lines.append(f"\n  [{dst.name}]")
                    lines.append(f"    Class: {dst.component_class}")
                    if dst.table_name:
                        lines.append(f"    Table: {dst.table_name}")
                    if dst.connection_manager:
                        lines.append(f"    Connection: {dst.connection_manager}")
                    if dst.has_error_output:
                        lines.append(f"    Has Error Output: Yes")

            # Data paths
            lines.append(f"\n3.4 DATA PATHS")
            lines.append(f"{'-'*50}")
            for path in dft.paths:
                lines.append(f"  {path.name}")
                lines.append(f"    From: {self._extract_component_short(path.source_ref_id)}")
                lines.append(f"    To:   {self._extract_component_short(path.destination_ref_id)}")

        return '\n'.join(lines)

    def _generate_error_handling_section(self) -> str:
        """Generate error handling section."""
        lines = []
        lines.append("=" * 80)
        lines.append(" 4. ERROR HANDLING STRATEGY")
        lines.append("=" * 80)

        if self.package.error_handling:
            eh = self.package.error_handling

            lines.append(f"\n4.1 LOGGING CONFIGURATION")
            lines.append(f"{'-'*50}")
            lines.append(f"  Logging Mode: {eh.logging_mode or 'Default'}")
            lines.append(f"  Fail on Failure: {'Yes' if eh.fail_package_on_failure else 'No'}")
            lines.append(f"  Max Error Count: {eh.max_error_count}")

            if eh.logged_events:
                lines.append(f"\n  Logged Events:")
                for event in eh.logged_events:
                    lines.append(f"    - {event}")

            if eh.event_handlers:
                lines.append(f"\n4.2 EVENT HANDLERS")
                lines.append(f"{'-'*50}")
                for handler in eh.event_handlers:
                    lines.append(f"\n  [{handler.event_name}]")
                    lines.append(f"    Tasks:")
                    for task in handler.executables:
                        lines.append(f"      - {task.get('name', 'Unknown')}")
                        if task.get('description'):
                            lines.append(f"        {task.get('description')}")
                        if task.get('to_address'):
                            lines.append(f"        Recipients: {task.get('to_address')}")

        return '\n'.join(lines)

    def _generate_database_objects_section(self) -> str:
        """Generate database objects section."""
        lines = []
        lines.append("=" * 80)
        lines.append(" 5. DATABASE OBJECTS")
        lines.append("=" * 80)

        # Group by type
        tables = [o for o in self.package.database_objects if o.object_type == 'Table']
        procedures = [o for o in self.package.database_objects if o.object_type == 'StoredProcedure']
        functions = [o for o in self.package.database_objects if o.object_type == 'Function']

        # Tables
        lines.append(f"\n5.1 TABLES ({len(tables)} found)")
        lines.append(f"{'-'*50}")
        lines.append(f"  {'Schema':<15} {'Name':<35} {'Usage':<15}")
        lines.append(f"  {'-'*15} {'-'*35} {'-'*15}")
        for obj in tables:
            lines.append(f"  {obj.schema:<15} {obj.name:<35} {obj.usage:<15}")

        # Stored Procedures
        if procedures:
            lines.append(f"\n5.2 STORED PROCEDURES ({len(procedures)} found)")
            lines.append(f"{'-'*50}")
            lines.append(f"  {'Schema':<15} {'Name':<35} {'Usage':<15}")
            lines.append(f"  {'-'*15} {'-'*35} {'-'*15}")
            for obj in procedures:
                lines.append(f"  {obj.schema:<15} {obj.name:<35} {obj.usage:<15}")

        # Functions
        if functions:
            lines.append(f"\n5.3 FUNCTIONS ({len(functions)} found)")
            lines.append(f"{'-'*50}")
            lines.append(f"  {'Schema':<15} {'Name':<35} {'Usage':<15}")
            lines.append(f"  {'-'*15} {'-'*35} {'-'*15}")
            for obj in functions:
                lines.append(f"  {obj.schema:<15} {obj.name:<35} {obj.usage:<15}")

        return '\n'.join(lines)

    def _generate_diagrams_section(self) -> str:
        """Generate diagrams section."""
        lines = []
        lines.append("=" * 80)
        lines.append(" 6. DATA FLOW DIAGRAMS")
        lines.append("=" * 80)

        # Control flow diagram
        lines.append(self.diagram_gen._generate_ascii_control_flow())

        # Data flow diagrams
        for dft in self.package.data_flow_tasks:
            lines.append(self.diagram_gen._generate_ascii_data_flow(dft))

        # Execution order
        lines.append(self.diagram_gen.generate_execution_order_diagram())

        # Routing logic
        lines.append(self.diagram_gen.generate_routing_logic_diagram())

        return '\n'.join(lines)

    def _generate_thresholds_alerts_section(self) -> str:
        """Generate thresholds and alerts section."""
        lines = []
        lines.append("=" * 80)
        lines.append(" 7. CRITICAL THRESHOLDS AND ALERTS")
        lines.append("=" * 80)

        # Thresholds
        lines.append(f"\n7.1 THRESHOLDS ({len(self.package.thresholds)} found)")
        lines.append(f"{'-'*60}")
        lines.append(f"  {'Name':<30} {'Value':<15} {'Category':<15}")
        lines.append(f"  {'-'*30} {'-'*15} {'-'*15}")
        for threshold in self.package.thresholds:
            value = str(threshold.value)[:15] if threshold.value else 'N/A'
            lines.append(f"  {threshold.name:<30} {value:<15} {threshold.category:<15}")

        # Alerts
        if self.package.alerts:
            lines.append(f"\n7.2 ALERTS ({len(self.package.alerts)} found)")
            lines.append(f"{'-'*60}")
            lines.append(f"  {'Name':<30} {'Type':<15} {'Priority':<10} {'Category':<10}")
            lines.append(f"  {'-'*30} {'-'*15} {'-'*10} {'-'*10}")
            for alert in self.package.alerts:
                lines.append(f"  {alert.name:<30} {alert.alert_type:<15} {alert.priority:<10} {alert.category:<10}")
                if alert.recipients:
                    lines.append(f"    Recipients: {alert.recipients}")

        return '\n'.join(lines)

    def _generate_json_report(self) -> str:
        """Generate JSON report."""
        data = {
            'metadata': {
                'name': self.package.metadata.name,
                'dtsid': self.package.metadata.dtsid,
                'creation_date': self.package.metadata.creation_date,
                'creator_name': self.package.metadata.creator_name,
                'version_build': self.package.metadata.version_build,
                'package_format_version': self.package.metadata.package_format_version
            },
            'connection_managers': [
                {
                    'name': c.name,
                    'type': c.connection_type,
                    'server': c.server,
                    'database': c.database
                }
                for c in self.package.connection_managers
            ],
            'variables': [
                {
                    'name': v.name,
                    'namespace': v.namespace,
                    'data_type': v.data_type,
                    'value': v.value
                }
                for v in self.package.variables
            ],
            'parameters': [
                {
                    'name': p.name,
                    'data_type': p.data_type,
                    'value': p.value,
                    'sensitive': p.sensitive
                }
                for p in self.package.parameters
            ],
            'control_flow_stages': [
                {
                    'order': s.order,
                    'name': s.name,
                    'type': s.stage_type,
                    'description': s.description,
                    'condition': s.condition,
                    'tasks': s.tasks
                }
                for s in self.package.control_flow_stages
            ],
            'data_flow_tasks': [
                {
                    'name': dft.name,
                    'description': dft.description,
                    'components': [
                        {
                            'name': c.name,
                            'type': c.component_type,
                            'class': c.component_class,
                            'sql_command': c.sql_command,
                            'table_name': c.table_name,
                            'output_columns': [
                                {'name': col.name, 'expression': col.expression}
                                for col in c.output_columns
                            ],
                            'conditional_outputs': [
                                {
                                    'name': co.name,
                                    'expression': co.expression,
                                    'is_default': co.is_default
                                }
                                for co in c.conditional_outputs
                            ]
                        }
                        for c in dft.components
                    ],
                    'paths': [
                        {
                            'name': p.name,
                            'source': p.source_ref_id,
                            'destination': p.destination_ref_id
                        }
                        for p in dft.paths
                    ]
                }
                for dft in self.package.data_flow_tasks
            ],
            'error_handling': {
                'logging_mode': self.package.error_handling.logging_mode if self.package.error_handling else None,
                'logged_events': self.package.error_handling.logged_events if self.package.error_handling else [],
                'event_handlers': [
                    {
                        'event_name': h.event_name,
                        'tasks': h.executables
                    }
                    for h in self.package.error_handling.event_handlers
                ] if self.package.error_handling else []
            },
            'database_objects': [
                {
                    'name': o.name,
                    'type': o.object_type,
                    'schema': o.schema,
                    'usage': o.usage
                }
                for o in self.package.database_objects
            ],
            'thresholds': [
                {
                    'name': t.name,
                    'value': t.value,
                    'category': t.category,
                    'data_type': t.data_type
                }
                for t in self.package.thresholds
            ],
            'alerts': [
                {
                    'name': a.name,
                    'type': a.alert_type,
                    'category': a.category,
                    'priority': a.priority,
                    'recipients': a.recipients
                }
                for a in self.package.alerts
            ]
        }

        return json.dumps(data, indent=2, default=str)

    def _extract_names(self, refs: list) -> list:
        """Extract clean names from ref IDs."""
        names = []
        for ref in refs:
            parts = ref.split('\\')
            names.append(parts[-1] if parts else ref)
        return names

    def _extract_component_short(self, ref_id: str) -> str:
        """Extract short component name from ref ID."""
        parts = ref_id.split('\\')
        if len(parts) > 1:
            return parts[-1].split('.')[0]
        return ref_id

    def save_report(self, output_path: str, output_format: str = 'text') -> None:
        """Save report to file."""
        report = self.generate_full_report(output_format)

        ext_map = {
            'text': '.txt',
            'markdown': '.md',
            'json': '.json'
        }

        path = Path(output_path)
        if path.suffix == '':
            path = path.with_suffix(ext_map.get(output_format, '.txt'))

        path.write_text(report, encoding='utf-8')
        print(f"Report saved to: {path}")

    def save_all_formats(self, output_dir: str) -> None:
        """Save reports in all formats."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        base_name = self.package.metadata.name.replace(' ', '_')

        for fmt in ['text', 'markdown', 'json']:
            ext = {'text': '.txt', 'markdown': '.md', 'json': '.json'}[fmt]
            file_path = output_path / f"{base_name}_report{ext}"
            self.save_report(str(file_path), fmt)
