"""
Diagram Generator for DTSX Packages

Generates visual representations of data flows and control flows
using Mermaid.js syntax and ASCII art.
"""

from typing import List, Dict, Optional
from .models import (
    DtsxPackage, DataFlowTask, DataFlowComponent, DataFlowPath,
    ControlFlowStage, DataFlowDiagram
)


class DiagramGenerator:
    """Generates diagrams for DTSX package visualization."""

    def __init__(self, package: DtsxPackage):
        """Initialize with a parsed DTSX package."""
        self.package = package

    def generate_all_diagrams(self) -> List[DataFlowDiagram]:
        """Generate all diagrams for the package."""
        diagrams = []

        # Generate control flow diagram
        cf_diagram = self.generate_control_flow_diagram()
        diagrams.append(cf_diagram)

        # Generate data flow diagrams for each DFT
        for dft in self.package.data_flow_tasks:
            df_diagram = self.generate_data_flow_diagram(dft)
            diagrams.append(df_diagram)

        return diagrams

    def generate_control_flow_diagram(self) -> DataFlowDiagram:
        """Generate a control flow diagram."""
        components = [stage.name for stage in self.package.control_flow_stages]
        paths = []

        # Build paths from precedence constraints
        for constraint in self.package.raw_precedence_constraints:
            from_name = self._extract_stage_name(constraint.from_ref)
            to_name = self._extract_stage_name(constraint.to_ref)
            label = ""
            if constraint.expression:
                label = constraint.expression.replace('@[User::', '').replace(']', '').replace('==', '=')
            paths.append((from_name, to_name, label))

        mermaid = self._generate_mermaid_flowchart(
            "Control Flow",
            components,
            paths,
            direction="TB"
        )

        ascii_diagram = self._generate_ascii_control_flow()

        return DataFlowDiagram(
            name="Control Flow",
            components=components,
            paths=paths,
            mermaid_code=mermaid,
            ascii_diagram=ascii_diagram
        )

    def generate_data_flow_diagram(self, dft: DataFlowTask) -> DataFlowDiagram:
        """Generate a data flow diagram for a specific data flow task."""
        components = [comp.name for comp in dft.components]
        paths = []

        # Build component lookup
        comp_lookup = {comp.ref_id: comp.name for comp in dft.components}

        # Also try to match by partial ref_id (output refs)
        for comp in dft.components:
            for suffix in ['.Outputs', '.Inputs']:
                for variant in [f'{comp.ref_id}{suffix}', comp.ref_id]:
                    comp_lookup[variant] = comp.name

        # Build paths
        for path in dft.paths:
            # Extract component names from path refs
            source_name = self._extract_component_name(path.source_ref_id, dft.components)
            dest_name = self._extract_component_name(path.destination_ref_id, dft.components)

            if source_name and dest_name:
                paths.append((source_name, dest_name, path.name))

        mermaid = self._generate_mermaid_flowchart(
            dft.name,
            components,
            paths,
            direction="TB",
            component_types=self._get_component_types(dft)
        )

        ascii_diagram = self._generate_ascii_data_flow(dft)

        return DataFlowDiagram(
            name=dft.name,
            components=components,
            paths=paths,
            mermaid_code=mermaid,
            ascii_diagram=ascii_diagram
        )

    def _extract_stage_name(self, ref_id: str) -> str:
        """Extract stage name from ref_id."""
        # Format: Package\StageName
        parts = ref_id.split('\\')
        return parts[-1] if parts else ref_id

    def _extract_component_name(self, ref_id: str, components: List[DataFlowComponent]) -> Optional[str]:
        """Extract component name from ref_id."""
        for comp in components:
            if comp.ref_id in ref_id or comp.name in ref_id:
                return comp.name
        # Try partial match
        parts = ref_id.split('\\')
        if len(parts) > 1:
            comp_part = parts[-1].split('.')[0] if '.' in parts[-1] else parts[-1]
            for comp in components:
                if comp_part == comp.name or comp_part in comp.ref_id:
                    return comp.name
        return None

    def _get_component_types(self, dft: DataFlowTask) -> Dict[str, str]:
        """Get component types for styling."""
        return {comp.name: comp.component_type for comp in dft.components}

    def _generate_mermaid_flowchart(self, title: str, components: List[str],
                                     paths: List[tuple], direction: str = "TB",
                                     component_types: Dict[str, str] = None) -> str:
        """Generate Mermaid.js flowchart syntax."""
        lines = [f"---"]
        lines.append(f"title: {title}")
        lines.append("---")
        lines.append(f"flowchart {direction}")

        # Add subgraph for title
        lines.append(f"    subgraph {self._sanitize_id(title)}[\"{title}\"]")

        # Define node styles based on component type
        if component_types is None:
            component_types = {}

        # Add nodes with appropriate shapes
        for comp in components:
            comp_id = self._sanitize_id(comp)
            comp_type = component_types.get(comp, 'default')

            if comp_type == 'Source':
                lines.append(f"        {comp_id}[(\"{comp}\")]")  # Cylinder for source
            elif comp_type == 'Destination':
                lines.append(f"        {comp_id}[[\"{comp}\"]]")  # Subroutine shape for dest
            elif 'Transform' in comp_type:
                lines.append(f"        {comp_id}{{\"{comp}\"}}")  # Diamond for transform
            else:
                lines.append(f"        {comp_id}[\"{comp}\"]")  # Default rectangle

        lines.append("    end")
        lines.append("")

        # Add paths/edges
        for source, dest, label in paths:
            source_id = self._sanitize_id(source)
            dest_id = self._sanitize_id(dest)

            if label:
                lines.append(f"    {source_id} -->|{label}| {dest_id}")
            else:
                lines.append(f"    {source_id} --> {dest_id}")

        # Add styling
        lines.append("")
        lines.append("    %% Styling")
        lines.append("    classDef source fill:#e1f5fe,stroke:#01579b")
        lines.append("    classDef destination fill:#e8f5e9,stroke:#1b5e20")
        lines.append("    classDef transform fill:#fff3e0,stroke:#e65100")

        # Apply styles
        for comp in components:
            comp_id = self._sanitize_id(comp)
            comp_type = component_types.get(comp, 'default')

            if comp_type == 'Source':
                lines.append(f"    class {comp_id} source")
            elif comp_type == 'Destination':
                lines.append(f"    class {comp_id} destination")
            elif 'Transform' in comp_type:
                lines.append(f"    class {comp_id} transform")

        return '\n'.join(lines)

    def _sanitize_id(self, name: str) -> str:
        """Sanitize a name for use as Mermaid node ID."""
        # Replace special characters
        result = name.replace(' ', '_').replace('-', '_').replace('.', '_')
        result = ''.join(c for c in result if c.isalnum() or c == '_')
        return result

    def _generate_ascii_control_flow(self) -> str:
        """Generate ASCII art for control flow."""
        lines = []
        lines.append("=" * 60)
        lines.append(" CONTROL FLOW DIAGRAM")
        lines.append("=" * 60)
        lines.append("")

        max_width = max(len(stage.name) for stage in self.package.control_flow_stages) if self.package.control_flow_stages else 20
        box_width = max_width + 4

        for i, stage in enumerate(self.package.control_flow_stages):
            # Draw stage box
            lines.append("    " + "+" + "-" * box_width + "+")
            lines.append("    " + "|" + stage.name.center(box_width) + "|")
            lines.append("    " + "|" + f"({stage.stage_type})".center(box_width) + "|")
            lines.append("    " + "+" + "-" * box_width + "+")

            # Draw arrow to next stage
            if i < len(self.package.control_flow_stages) - 1:
                # Check for condition
                next_stage = self.package.control_flow_stages[i + 1]
                condition = next_stage.condition

                lines.append("    " + " " * (box_width // 2 + 1) + "|")
                if condition:
                    cond_text = condition.replace('@[User::', '').replace(']', '').replace('==', '=')
                    lines.append("    " + " " * 4 + f"[{cond_text}]")
                lines.append("    " + " " * (box_width // 2 + 1) + "V")
                lines.append("")

        return '\n'.join(lines)

    def _generate_ascii_data_flow(self, dft: DataFlowTask) -> str:
        """Generate ASCII art for a data flow task."""
        lines = []
        lines.append("=" * 70)
        lines.append(f" DATA FLOW: {dft.name}")
        lines.append("=" * 70)
        lines.append("")

        # Group components by type
        sources = [c for c in dft.components if c.component_type == 'Source']
        transforms = [c for c in dft.components if c.component_type == 'Transform']
        destinations = [c for c in dft.components if c.component_type == 'Destination']

        # Draw sources
        if sources:
            lines.append("SOURCES:")
            for source in sources:
                lines.append(f"    [({source.name})]")
                if source.sql_command:
                    # Show abbreviated SQL
                    sql_preview = source.sql_command.strip()[:50].replace('\n', ' ')
                    lines.append(f"        SQL: {sql_preview}...")
            lines.append("        |")
            lines.append("        V")
            lines.append("")

        # Draw transforms
        if transforms:
            lines.append("TRANSFORMATIONS:")
            for i, transform in enumerate(transforms):
                # Determine symbol based on component class
                if 'DerivedColumn' in transform.component_class:
                    symbol = "{DER}"
                elif 'ConditionalSplit' in transform.component_class:
                    symbol = "{SPLIT}"
                elif 'Lookup' in transform.component_class:
                    symbol = "{LKP}"
                elif 'Multicast' in transform.component_class:
                    symbol = "{MCT}"
                elif 'RowCount' in transform.component_class:
                    symbol = "{RC}"
                elif 'DataConvert' in transform.component_class:
                    symbol = "{DCV}"
                else:
                    symbol = "{TRF}"

                lines.append(f"    {symbol} {transform.name}")

                # Show derived columns
                if transform.output_columns:
                    for col in transform.output_columns[:3]:
                        if col.expression:
                            lines.append(f"          -> {col.name}: {col.expression[:40]}...")

                # Show conditional outputs
                if transform.conditional_outputs:
                    for cond in transform.conditional_outputs:
                        if cond.expression:
                            lines.append(f"          |-> [{cond.name}]: {cond.expression}")
                        elif cond.is_default:
                            lines.append(f"          |-> [{cond.name}]: (Default)")

                if i < len(transforms) - 1:
                    lines.append("        |")
                    lines.append("        V")

            lines.append("        |")
            lines.append("        V")
            lines.append("")

        # Draw destinations
        if destinations:
            lines.append("DESTINATIONS:")
            for dest in destinations:
                table = dest.table_name or "Unknown Table"
                lines.append(f"    [[{dest.name}]]")
                lines.append(f"        -> {table}")
            lines.append("")

        # Draw path summary
        lines.append("-" * 70)
        lines.append("DATA PATHS:")
        for path in dft.paths:
            source = self._extract_component_name(path.source_ref_id, dft.components) or "?"
            dest = self._extract_component_name(path.destination_ref_id, dft.components) or "?"
            lines.append(f"    {source} --> {dest}")

        return '\n'.join(lines)

    def generate_execution_order_diagram(self) -> str:
        """Generate a diagram showing execution order."""
        lines = []
        lines.append("=" * 60)
        lines.append(" EXECUTION ORDER")
        lines.append("=" * 60)
        lines.append("")

        # Build execution order from precedence constraints
        execution_order = []
        remaining_stages = list(self.package.control_flow_stages)

        # Find starting stages (no incoming precedence)
        start_stages = []
        for stage in remaining_stages:
            if not stage.precedence_from:
                start_stages.append(stage)

        # Add stages in order
        current_order = 1
        for stage in self.package.control_flow_stages:
            lines.append(f"Step {current_order}: {stage.name}")
            lines.append(f"         Type: {stage.stage_type}")
            if stage.description:
                lines.append(f"         Description: {stage.description}")
            if stage.condition:
                cond = stage.condition.replace('@[User::', '').replace(']', '')
                lines.append(f"         Condition: {cond}")

            # List tasks within the stage
            if stage.tasks:
                lines.append("         Tasks:")
                for task in stage.tasks:
                    lines.append(f"           - {task.get('name', 'Unknown')}")

            lines.append("")
            current_order += 1

        return '\n'.join(lines)

    def generate_routing_logic_diagram(self) -> str:
        """Generate a diagram showing data routing logic."""
        lines = []
        lines.append("=" * 70)
        lines.append(" DATA ROUTING LOGIC")
        lines.append("=" * 70)
        lines.append("")

        for dft in self.package.data_flow_tasks:
            lines.append(f"Data Flow: {dft.name}")
            lines.append("-" * 50)
            lines.append("")

            # Find conditional splits and their routing
            for comp in dft.components:
                if comp.conditional_outputs:
                    lines.append(f"  Routing Component: {comp.name}")
                    lines.append(f"  Type: {comp.component_class.split('.')[-1]}")
                    lines.append("")

                    for i, cond in enumerate(comp.conditional_outputs, 1):
                        if cond.is_default:
                            lines.append(f"    Route {i}: {cond.name}")
                            lines.append(f"      Condition: DEFAULT (all unmatched rows)")
                        else:
                            lines.append(f"    Route {i}: {cond.name}")
                            if cond.friendly_expression:
                                lines.append(f"      Condition: {cond.friendly_expression}")
                            elif cond.expression:
                                lines.append(f"      Condition: {cond.expression}")

                        # Find where this route goes
                        for path in dft.paths:
                            if cond.name in path.source_ref_id:
                                dest = self._extract_component_name(
                                    path.destination_ref_id, dft.components
                                )
                                if dest:
                                    lines.append(f"      Destination: {dest}")

                        lines.append("")

                # Show lookup no-match routing
                if 'Lookup' in comp.component_class:
                    lines.append(f"  Lookup: {comp.name}")
                    lines.append(f"    Match Output: Rows with matching reference data")
                    lines.append(f"    No Match Output: Rows without matching reference data")

                    # Find destinations
                    for path in dft.paths:
                        if comp.name in path.source_ref_id:
                            dest = self._extract_component_name(
                                path.destination_ref_id, dft.components
                            )
                            if dest:
                                if 'Match' in path.name:
                                    lines.append(f"      -> Match goes to: {dest}")
                                elif 'NoMatch' in path.name or 'No Match' in path.name:
                                    lines.append(f"      -> No Match goes to: {dest}")

                    lines.append("")

                # Show multicast routing
                if 'Multicast' in comp.component_class:
                    lines.append(f"  Multicast: {comp.name}")
                    lines.append(f"    Sends all rows to multiple destinations:")

                    for path in dft.paths:
                        if comp.name in path.source_ref_id:
                            dest = self._extract_component_name(
                                path.destination_ref_id, dft.components
                            )
                            if dest:
                                lines.append(f"      -> {dest}")

                    lines.append("")

            lines.append("")

        return '\n'.join(lines)
