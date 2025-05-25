# HDFS Management Scripts

This directory contains 11 shell scripts for managing files and directories in HDFS (Hadoop Distributed File System). All scripts are written for zsh and include comprehensive error handling and user feedback.

## Scripts Overview

| Script | Purpose | Key Features |
|--------|---------|--------------|
| `01_upload_to_hdfs.sh` | Upload files to HDFS | Supports overwrite/append modes |
| `02_download_from_hdfs.sh` | Download files from HDFS | Auto-rename if local file exists |
| `03_cat_hdfs_file.sh` | Display file content | Shows HDFS file content in terminal |
| `04_show_hdfs_file_info.sh` | Show file information | Displays permissions, size, timestamps |
| `05_list_hdfs_dir_recursive.sh` | List directory recursively | Shows all files with statistics |
| `06_manage_hdfs_file.sh` | Create/delete files | Auto-creates parent directories |
| `07_manage_hdfs_dir.sh` | Create/delete directories | Supports force delete for non-empty dirs |
| `08_append_to_hdfs_file.sh` | Append content to files | Supports beginning/end positioning |
| `09_delete_hdfs_file.sh` | Delete specific files | Interactive confirmation |
| `10_delete_hdfs_dir.sh` | Delete directories | Interactive confirmation with force option |
| `11_move_hdfs_file.sh` | Move/rename files | Handles directory destinations |

## Prerequisites

- Hadoop cluster must be running
- HDFS client commands (`hdfs dfs`) must be available
- User must have appropriate HDFS permissions
- zsh shell (scripts use `#!/bin/zsh`)

## Usage Instructions

### 1. Upload File to HDFS
```bash
./01_upload_to_hdfs.sh <local_file> <hdfs_path> [mode]
```
- **mode**: `overwrite` (default) or `append`
- **Example**: `./01_upload_to_hdfs.sh /tmp/data.txt /user/data/data.txt overwrite`

### 2. Download File from HDFS
```bash
./02_download_from_hdfs.sh <hdfs_path> <local_path>
```
- Auto-renames download if local file exists
- **Example**: `./02_download_from_hdfs.sh /user/data/data.txt /tmp/downloaded.txt`

### 3. Display File Content
```bash
./03_cat_hdfs_file.sh <hdfs_file_path>
```
- **Example**: `./03_cat_hdfs_file.sh /user/data/data.txt`

### 4. Show File Information
```bash
./04_show_hdfs_file_info.sh <hdfs_file_path>
```
- Shows permissions, size, modification time, block size, replication
- **Example**: `./04_show_hdfs_file_info.sh /user/data/data.txt`

### 5. List Directory Recursively
```bash
./05_list_hdfs_dir_recursive.sh <hdfs_directory_path>
```
- Lists all files and subdirectories with statistics
- **Example**: `./05_list_hdfs_dir_recursive.sh /user/data`

### 6. Manage Files (Create/Delete)
```bash
./06_manage_hdfs_file.sh <operation> <hdfs_file_path> [content]
```
- **operation**: `create` or `delete`
- **Examples**:
  - `./06_manage_hdfs_file.sh create /user/data/newfile.txt`
  - `./06_manage_hdfs_file.sh create /user/data/newfile.txt "Hello World"`
  - `./06_manage_hdfs_file.sh delete /user/data/newfile.txt`

### 7. Manage Directories (Create/Delete)
```bash
./07_manage_hdfs_dir.sh <operation> <hdfs_directory_path> [force]
```
- **operation**: `create` or `delete`
- **force**: Use with delete to remove non-empty directories
- **Examples**:
  - `./07_manage_hdfs_dir.sh create /user/data/newdir`
  - `./07_manage_hdfs_dir.sh delete /user/data/newdir`
  - `./07_manage_hdfs_dir.sh delete /user/data/newdir force`

### 8. Append Content to File
```bash
./08_append_to_hdfs_file.sh <hdfs_file_path> <content> [position]
```
- **position**: `end` (default) or `beginning`
- **Examples**:
  - `./08_append_to_hdfs_file.sh /user/data/file.txt "Footer content"`
  - `./08_append_to_hdfs_file.sh /user/data/file.txt "Header content" beginning`

### 9. Delete File
```bash
./09_delete_hdfs_file.sh <hdfs_file_path>
```
- Interactive confirmation required
- **Example**: `./09_delete_hdfs_file.sh /user/data/file.txt`

### 10. Delete Directory
```bash
./10_delete_hdfs_dir.sh <hdfs_directory_path> [force]
```
- Interactive confirmation required
- **force**: Delete non-empty directories
- **Examples**:
  - `./10_delete_hdfs_dir.sh /user/data/emptydir`
  - `./10_delete_hdfs_dir.sh /user/data/nonemptydir force`

### 11. Move/Rename File
```bash
./11_move_hdfs_file.sh <source_path> <destination_path>
```
- Supports moving to directories or renaming
- **Examples**:
  - `./11_move_hdfs_file.sh /user/data/file1.txt /user/backup/file1.txt`
  - `./11_move_hdfs_file.sh /user/data/file1.txt /user/newdir/`

## Common Features

### Error Handling
- All scripts validate input parameters
- Check for file/directory existence before operations
- Provide clear error messages
- Exit with appropriate status codes

### Safety Features
- Interactive confirmations for destructive operations
- Automatic backup/renaming to prevent overwrites
- Directory creation for missing parent paths
- Force flags for potentially dangerous operations

### User Feedback
- Progress indicators for long operations
- Detailed status messages
- File information display after operations
- Clear usage instructions and examples

## Troubleshooting

### Common Issues
1. **Permission Denied**: Ensure your user has proper HDFS permissions
2. **File Not Found**: Check file paths and HDFS connectivity
3. **Directory Exists**: Use appropriate flags or different paths
4. **Disk Space**: Ensure sufficient space in HDFS and local filesystem

### Verification Commands
```bash
# Check HDFS connectivity
hdfs dfs -ls /

# Check user permissions
hdfs dfs -ls /user

# Check available space
hdfs dfsadmin -report
```

## Security Notes

- Scripts include interactive confirmations for destructive operations
- Force delete options require explicit specification
- File overwrite operations require user confirmation
- All operations respect HDFS access controls

## Script Dependencies

- `hdfs` command-line client
- Standard Unix utilities: `dirname`, `basename`, `date`, `wc`, `grep`
- zsh shell features for parameter expansion and case statements