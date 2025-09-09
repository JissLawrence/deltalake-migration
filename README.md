# deltalake-migration
Migrating NHS Patient Administration System (PAS) legacy SQL Server data into Delta Lake using Azure Data Factory, PySpark, and Databricks. Includes Medallion Architecture design.
ENCRYPTED_CREDENTIAL_PATTERN = r'"encryptedCredential":\s*"[^"]*"'

def sanitize_json_content(content):
    """Sanitize JSON content by replacing sensitive information."""
    
    # Replace encrypted credentials with placeholder
    content = re.sub(
        ENCRYPTED_CREDENTIAL_PATTERN, 
        '"encryptedCredential": "YOUR_ENCRYPTED_CREDENTIAL_HERE"',
        content
    )
    
    # Replace other sensitive information
    for old, new in REPLACEMENTS.items():
        content = content.replace(old, new)
    
    return content

def sanitize_file(file_path):
    """Sanitize a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Sanitize content
        sanitized_content = sanitize_json_content(content)
        
        # Create output directory
        output_dir = Path('sanitized_configs')
        output_dir.mkdir(exist_ok=True)
        
        # Write sanitized file
        output_file = output_dir / Path(file_path).name
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(sanitized_content)
        
        print(f"✅ Sanitized: {file_path} -> {output_file}")
        
    except Exception as e:
        print(f"❌ Error processing {file_path}: {e}")

def main():
    """Main function to process all JSON files in current directory."""
    
    # Find all JSON files
    json_files = list(Path('.').glob('*.json'))
    
    if not json_files:
        print("No JSON files found in current directory")
        return
    
    print(f"Found {len(json_files)} JSON files to sanitize:")
    for file_path in json_files:
        print(f"  - {file_path}")
    
    print("\nSanitizing files...")
    
    for file_path in json_files:
        sanitize_file(file_path)
    
    print(f"\n✅ Sanitization complete! Check the 'sanitized_configs' directory.")
    print("\n⚠️  IMPORTANT: Review the sanitized files before publishing to ensure no sensitive data remains!")

if __name__ == "__main__":
    main()
