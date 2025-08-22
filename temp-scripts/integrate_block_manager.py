#!/usr/bin/env python3
"""
Integration script to update funbuns CLI with block management capabilities.
Updates the codebase to use "blocks" terminology and preferred structure.
"""

import os
from pathlib import Path

def update_main_cli():
    """Update __main__.py to integrate block management commands."""
    
    main_file = Path("src/funbuns/__main__.py")
    
    if not main_file.exists():
        print(f"❌ {main_file} not found")
        return
    
    print(f"🔧 Updating {main_file} with block management integration...")
    
    # Read current content
    with open(main_file, 'r') as f:
        content = f.read()
    
    # Add block management imports at top
    if "from .block_manager import BlockManager" not in content:
        # Find the imports section and add our import
        import_insertion = content.find("from .utils import")
        if import_insertion != -1:
            # Add after the utils import
            utils_line_end = content.find('\n', import_insertion)
            new_content = (content[:utils_line_end] + 
                          "\nfrom .block_manager import BlockManager" + 
                          content[utils_line_end:])
            content = new_content
    
    # Add block management arguments to parser
    if "--show-blocks" not in content:
        # Find where arguments are defined
        no_table_arg = content.find("parser.add_argument('--no-table'")
        if no_table_arg != -1:
            # Find the end of the no-table argument
            arg_end = content.find('\n', no_table_arg)
            block_args = '''
    parser.add_argument('--show-blocks', action='store_true',
                       help='Show summary of data blocks using glob patterns')
    parser.add_argument('--convert-blocks', action='store_true',
                       help='Convert current data to block format with preferred naming')
    parser.add_argument('--block-size', type=int, default=500000,
                       help='Target primes per block (default: 500,000)')
    parser.add_argument('--reconfigure-blocks', type=int, metavar='PRIMES',
                       help='Reconfigure existing blocks to N primes per block')'''
            
            new_content = content[:arg_end] + block_args + content[arg_end:]
            content = new_content
    
    # Add block management handlers
    if "args.show_blocks" not in content:
        # Find where other command handlers are
        aggregate_handler = content.find("if args.aggregate:")
        if aggregate_handler != -1:
            # Add block handlers before aggregate
            block_handlers = '''
    # Handle block management commands
    if args.show_blocks:
        from .block_manager import BlockManager
        manager = BlockManager()
        # Try blocks first, fall back to runs
        block_files = list(manager.blocks_dir.glob("*.parquet"))
        use_blocks = len(block_files) > 0
        manager.show_block_summary(use_blocks=use_blocks)
        return
    
    if args.convert_blocks:
        from .block_manager import BlockManager
        manager = BlockManager()
        manager.convert_runs_to_blocks(target_prime_count=args.block_size)
        return
    
    if args.reconfigure_blocks:
        from .block_manager import BlockManager
        manager = BlockManager()
        manager.reconfigure_block_size(args.reconfigure_blocks)
        return
    
'''
            new_content = content[:aggregate_handler] + block_handlers + content[aggregate_handler:]
            content = new_content
    
    # Write updated content
    with open(main_file, 'w') as f:
        f.write(content)
    
    print("  ✅ Updated main CLI with block management commands")

def create_block_manager_module():
    """Copy block_manager.py to the main module."""
    
    source = Path("temp-scripts/block_manager.py")
    dest = Path("src/funbuns/block_manager.py")
    
    if not source.exists():
        print(f"❌ {source} not found")
        return
    
    print(f"📁 Copying {source} to {dest}...")
    
    # Read the source content
    with open(source, 'r') as f:
        content = f.read()
    
    # Make it a proper module (remove main execution)
    content = content.replace('''
if __name__ == "__main__":
    main()''', '')
    
    # Write to destination
    with open(dest, 'w') as f:
        f.write(content)
    
    print("  ✅ Created block_manager module")

def update_utils_terminology():
    """Update utils.py to use 'blocks' instead of 'runs' terminology."""
    
    utils_file = Path("src/funbuns/utils.py")
    
    if not utils_file.exists():
        print(f"❌ {utils_file} not found")
        return
    
    print(f"🔧 Updating {utils_file} terminology...")
    
    with open(utils_file, 'r') as f:
        content = f.read()
    
    # Update function names and documentation
    replacements = [
        ("get_run_file", "get_block_file"),
        ("get_all_run_files", "get_all_block_files"), 
        ("show_run_files_summary", "show_block_files_summary"),
        ("aggregate_run_files", "aggregate_block_files"),
        ("run files", "block files"),
        ("Run files", "Block files"),
        ("runs/", "blocks/"),
        ("pparts_run_", "pp_b"),
        ("run file", "block file"),
        ("Run file", "Block file")
    ]
    
    for old, new in replacements:
        content = content.replace(old, new)
    
    # Update directory creation
    content = content.replace('/ "runs"', '/ "blocks"')
    
    with open(utils_file, 'w') as f:
        f.write(content)
    
    print("  ✅ Updated utils.py terminology")

def update_config_for_blocks():
    """Update pixi.toml configuration for blocks."""
    
    config_file = Path("pixi.toml")
    
    if not config_file.exists():
        print(f"❌ {config_file} not found")
        return
    
    print(f"🔧 Updating {config_file} configuration...")
    
    with open(config_file, 'r') as f:
        content = f.read()
    
    # Add block-related configuration
    if "default_block_size" not in content:
        # Find the [tool.funbuns] section
        tool_section = content.find("[tool.funbuns]")
        if tool_section != -1:
            # Find the end of the section
            section_end = content.find("\n\n", tool_section)
            if section_end == -1:
                section_end = len(content)
            
            # Add block configuration
            block_config = "\ndefault_block_size = 500000  # Target primes per block"
            new_content = content[:section_end] + block_config + content[section_end:]
            content = new_content
    
    with open(config_file, 'w') as f:
        f.write(content)
    
    print("  ✅ Updated configuration for blocks")

def show_integration_summary():
    """Show summary of what was integrated."""
    
    print("\n🎉 Block Management Integration Complete!")
    print("\n📋 New CLI Commands:")
    print("  pixi run funbuns --show-blocks           # Show block summary using glob patterns")
    print("  pixi run funbuns --convert-blocks        # Convert runs to preferred block format")
    print("  pixi run funbuns --reconfigure-blocks N  # Change block size to N primes per block")
    print("  pixi run funbuns --block-size N          # Set target block size for conversion")
    
    print("\n📁 File Structure:")
    print("  data/blocks/                             # Preferred block storage")
    print("    ├── pp_b001_p7368787.parquet           # Block 1, ends at prime 7,368,787")
    print("    ├── pp_b002_p15485863.parquet          # Block 2, ends at prime 15,485,863")
    print("    └── ...                                 # Sequential + end prime naming")
    print("  temp-scripts/                            # Utility scripts for reference")
    print("    ├── block_manager.py                   # Standalone block manager")
    print("    └── integrate_block_manager.py         # This integration script")
    
    print("\n🔗 Analysis Without Aggregation:")
    print("  pl.scan_parquet('data/blocks/*.parquet') # Works seamlessly")
    print("  funbuns --show-blocks                    # Uses glob patterns internally")
    
    print("\n⚙️  Configuration in pixi.toml:")
    print("  default_block_size = 500000              # Configurable block size")
    
    print("\n💡 Benefits:")
    print("  ✅ Git-friendly (no file size limits with proper block sizes)")
    print("  ✅ Rust parquet tool compatible")
    print("  ✅ Glob pattern analysis (*.parquet)")
    print("  ✅ No aggregation needed for most operations")
    print("  ✅ Configurable block sizes for optimal performance")

def main():
    print("🚀 Integrating Block Management into Funbuns CLI...")
    
    create_block_manager_module()
    update_main_cli()
    update_utils_terminology() 
    update_config_for_blocks()
    show_integration_summary()

if __name__ == "__main__":
    main()


