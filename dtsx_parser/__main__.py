#!/usr/bin/env python3
"""
DTSX Package Parser CLI

Command-line interface for parsing and analyzing SSIS packages.

Usage:
    python -m dtsx_parser <dtsx_file> [options]

Examples:
    python -m dtsx_parser package.dtsx
    python -m dtsx_parser package.dtsx --format markdown --output report.md
    python -m dtsx_parser package.dtsx --all-formats --output-dir ./reports
"""

import argparse
import sys
from pathlib import Path

from .parser import DtsxParser
from .report_generator import ReportGenerator
from .diagram_generator import DiagramGenerator


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Parse and analyze DTSX (SSIS) packages',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s package.dtsx                          Parse and display report
  %(prog)s package.dtsx -f markdown -o report.md Save as Markdown
  %(prog)s package.dtsx --all-formats -d ./out   Save all formats
  %(prog)s package.dtsx --diagrams-only          Show only diagrams
        """
    )

    parser.add_argument(
        'dtsx_file',
        help='Path to the DTSX package file'
    )

    parser.add_argument(
        '-f', '--format',
        choices=['text', 'markdown', 'json'],
        default='text',
        help='Output format (default: text)'
    )

    parser.add_argument(
        '-o', '--output',
        help='Output file path (prints to stdout if not specified)'
    )

    parser.add_argument(
        '-d', '--output-dir',
        help='Output directory for reports'
    )

    parser.add_argument(
        '--all-formats',
        action='store_true',
        help='Generate reports in all formats (text, markdown, json)'
    )

    parser.add_argument(
        '--diagrams-only',
        action='store_true',
        help='Generate only data flow diagrams'
    )

    parser.add_argument(
        '--mermaid',
        action='store_true',
        help='Output Mermaid diagram code'
    )

    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show only package summary'
    )

    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Verbose output'
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 1.0.0'
    )

    args = parser.parse_args()

    # Validate input file
    dtsx_path = Path(args.dtsx_file)
    if not dtsx_path.exists():
        print(f"Error: File not found: {args.dtsx_file}", file=sys.stderr)
        sys.exit(1)

    if not dtsx_path.suffix.lower() == '.dtsx':
        print(f"Warning: File does not have .dtsx extension", file=sys.stderr)

    try:
        # Parse the package
        if args.verbose:
            print(f"Parsing: {dtsx_path}", file=sys.stderr)

        dtsx_parser = DtsxParser(str(dtsx_path))
        package = dtsx_parser.parse()

        if args.verbose:
            print(f"Package parsed: {package.metadata.name}", file=sys.stderr)
            print(f"  Connections: {len(package.connection_managers)}", file=sys.stderr)
            print(f"  Variables: {len(package.variables)}", file=sys.stderr)
            print(f"  Control Flow Stages: {len(package.control_flow_stages)}", file=sys.stderr)
            print(f"  Data Flow Tasks: {len(package.data_flow_tasks)}", file=sys.stderr)

        # Generate output based on options
        if args.summary:
            print_summary(package)
        elif args.diagrams_only:
            diagram_gen = DiagramGenerator(package)
            if args.mermaid:
                diagrams = diagram_gen.generate_all_diagrams()
                for diagram in diagrams:
                    print(f"\n### {diagram.name}\n")
                    print(diagram.mermaid_code)
            else:
                print(diagram_gen._generate_ascii_control_flow())
                for dft in package.data_flow_tasks:
                    print(diagram_gen._generate_ascii_data_flow(dft))
                print(diagram_gen.generate_execution_order_diagram())
                print(diagram_gen.generate_routing_logic_diagram())
        elif args.all_formats:
            report_gen = ReportGenerator(package)
            output_dir = args.output_dir or '.'
            report_gen.save_all_formats(output_dir)
        else:
            report_gen = ReportGenerator(package)

            if args.output:
                report_gen.save_report(args.output, args.format)
            else:
                report = report_gen.generate_full_report(args.format)
                print(report)

    except ET.ParseError as e:
        print(f"Error parsing XML: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


def print_summary(package):
    """Print a brief package summary."""
    print("=" * 60)
    print(f" PACKAGE SUMMARY: {package.metadata.name}")
    print("=" * 60)
    print()
    print(f"  Created:     {package.metadata.creation_date or 'N/A'}")
    print(f"  Creator:     {package.metadata.creator_name or 'N/A'}")
    print(f"  Version:     {package.metadata.version_build or 'N/A'}")
    print()
    print("  COMPONENTS:")
    print(f"    Connection Managers:  {len(package.connection_managers)}")
    print(f"    Variables:            {len(package.variables)}")
    print(f"    Parameters:           {len(package.parameters)}")
    print(f"    Control Flow Stages:  {len(package.control_flow_stages)}")
    print(f"    Data Flow Tasks:      {len(package.data_flow_tasks)}")
    print(f"    Database Objects:     {len(package.database_objects)}")
    print(f"    Thresholds:           {len(package.thresholds)}")
    print(f"    Alerts:               {len(package.alerts)}")
    print()

    if package.control_flow_stages:
        print("  CONTROL FLOW STAGES:")
        for stage in package.control_flow_stages:
            cond = f" [Conditional]" if stage.condition else ""
            print(f"    {stage.order}. {stage.name} ({stage.stage_type}){cond}")
        print()

    if package.data_flow_tasks:
        print("  DATA FLOW TASKS:")
        for dft in package.data_flow_tasks:
            sources = len([c for c in dft.components if c.component_type == 'Source'])
            transforms = len([c for c in dft.components if c.component_type == 'Transform'])
            dests = len([c for c in dft.components if c.component_type == 'Destination'])
            print(f"    - {dft.name}")
            print(f"      Sources: {sources}, Transforms: {transforms}, Destinations: {dests}")
        print()

    if package.connection_managers:
        print("  CONNECTIONS:")
        for conn in package.connection_managers:
            db = f" -> {conn.database}" if conn.database else ""
            print(f"    - {conn.name} ({conn.connection_type}){db}")
        print()


# Import ET for exception handling
import xml.etree.ElementTree as ET


if __name__ == '__main__':
    main()
