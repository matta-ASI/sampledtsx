#!/usr/bin/env python3
"""
Example usage of the DTSX Parser library.

This script demonstrates how to use the dtsx_parser package to:
1. Parse a DTSX package
2. Access package components
3. Generate reports in different formats
4. Create data flow diagrams
"""

from dtsx_parser import DtsxParser, ReportGenerator, DiagramGenerator


def main():
    """Demonstrate DTSX parser usage."""

    # Path to the DTSX file
    dtsx_file = 'CreditCardTransactionProcessing.dtsx'

    print("=" * 70)
    print(" DTSX Parser Example Usage")
    print("=" * 70)
    print()

    # 1. Parse the package
    print("1. Parsing DTSX package...")
    parser = DtsxParser(dtsx_file)
    package = parser.parse()
    print(f"   Package Name: {package.metadata.name}")
    print(f"   Created: {package.metadata.creation_date}")
    print()

    # 2. Access package components
    print("2. Package Components:")
    print(f"   - Connection Managers: {len(package.connection_managers)}")
    for conn in package.connection_managers:
        print(f"     * {conn.name} ({conn.connection_type})")
    print()

    print(f"   - Variables: {len(package.variables)}")
    for var in package.variables[:5]:  # Show first 5
        print(f"     * {var.namespace}::{var.name} = {var.value}")
    if len(package.variables) > 5:
        print(f"     ... and {len(package.variables) - 5} more")
    print()

    print(f"   - Parameters: {len(package.parameters)}")
    for param in package.parameters:
        print(f"     * {param.name} = {param.value}")
    print()

    # 3. Control Flow Stages
    print("3. Control Flow Stages (Execution Order):")
    for stage in package.control_flow_stages:
        condition = f" [IF: {stage.condition}]" if stage.condition else ""
        print(f"   Stage {stage.order}: {stage.name} ({stage.stage_type}){condition}")
        for task in stage.tasks[:3]:  # Show first 3 tasks
            print(f"     - {task.get('name', 'Unknown')}")
        if len(stage.tasks) > 3:
            print(f"     ... and {len(stage.tasks) - 3} more tasks")
    print()

    # 4. Data Flow Tasks
    print("4. Data Flow Tasks:")
    for dft in package.data_flow_tasks:
        print(f"   {dft.name}:")
        sources = [c for c in dft.components if c.component_type == 'Source']
        transforms = [c for c in dft.components if c.component_type == 'Transform']
        destinations = [c for c in dft.components if c.component_type == 'Destination']

        print(f"     Sources ({len(sources)}):")
        for src in sources:
            print(f"       - {src.name}")

        print(f"     Transforms ({len(transforms)}):")
        for trn in transforms:
            class_name = trn.component_class.split('.')[-1]
            print(f"       - {trn.name} [{class_name}]")
            if trn.conditional_outputs:
                for co in trn.conditional_outputs:
                    expr = co.expression or "(Default)"
                    print(f"         -> Route: {co.name}: {expr[:40]}")

        print(f"     Destinations ({len(destinations)}):")
        for dst in destinations:
            table = dst.table_name or "N/A"
            print(f"       - {dst.name} -> {table}")
    print()

    # 5. Database Objects
    print("5. Database Objects:")
    tables = [o for o in package.database_objects if o.object_type == 'Table']
    procs = [o for o in package.database_objects if o.object_type == 'StoredProcedure']
    funcs = [o for o in package.database_objects if o.object_type == 'Function']

    print(f"   Tables ({len(tables)}):")
    for t in tables[:5]:
        print(f"     - {t.schema}.{t.name} [{t.usage}]")
    if len(tables) > 5:
        print(f"     ... and {len(tables) - 5} more")

    if procs:
        print(f"   Stored Procedures ({len(procs)}):")
        for p in procs:
            print(f"     - {p.schema}.{p.name}")

    if funcs:
        print(f"   Functions ({len(funcs)}):")
        for f in funcs:
            print(f"     - {f.schema}.{f.name}")
    print()

    # 6. Error Handling
    print("6. Error Handling Strategy:")
    if package.error_handling:
        print(f"   Logging Mode: {package.error_handling.logging_mode}")
        print(f"   Logged Events: {', '.join(package.error_handling.logged_events)}")
        print(f"   Event Handlers:")
        for handler in package.error_handling.event_handlers:
            print(f"     - {handler.event_name}: {len(handler.executables)} tasks")
    print()

    # 7. Thresholds
    print("7. Critical Thresholds:")
    for t in package.thresholds:
        print(f"   - {t.name}: {t.value} [{t.category}]")
    print()

    # 8. Alerts
    print("8. Alerts Configuration:")
    for alert in package.alerts:
        print(f"   - {alert.name} ({alert.alert_type}) - {alert.priority}")
    print()

    # 9. Generate Reports
    print("9. Generating Reports...")
    report_gen = ReportGenerator(package)

    # Save in all formats
    report_gen.save_report('output_report.txt', 'text')
    report_gen.save_report('output_report.md', 'markdown')
    report_gen.save_report('output_report.json', 'json')
    print()

    # 10. Generate Diagrams
    print("10. Generating Diagrams...")
    diagram_gen = DiagramGenerator(package)
    diagrams = diagram_gen.generate_all_diagrams()

    for diagram in diagrams:
        print(f"    Generated: {diagram.name}")
        # Save Mermaid code
        with open(f"diagram_{diagram.name.replace(' ', '_')}.mmd", 'w') as f:
            f.write(diagram.mermaid_code)

    # Print execution order
    print()
    print("=" * 70)
    print(" EXECUTION ORDER DIAGRAM")
    print("=" * 70)
    print(diagram_gen.generate_execution_order_diagram())

    # Print routing logic
    print()
    print("=" * 70)
    print(" DATA ROUTING LOGIC")
    print("=" * 70)
    print(diagram_gen.generate_routing_logic_diagram())

    print()
    print("=" * 70)
    print(" DONE!")
    print("=" * 70)
    print()
    print("Generated files:")
    print("  - output_report.txt    (Text report)")
    print("  - output_report.md     (Markdown report)")
    print("  - output_report.json   (JSON report)")
    print("  - diagram_*.mmd        (Mermaid diagram files)")


if __name__ == '__main__':
    main()
