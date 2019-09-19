# File System
A file system written from scratch in C, highlighting memory and resource management using synchronisation primitives. 

## Files and Components

### file_data
File containing the contents of all virtual files stored.

### directory_table
File that maps filenames to where the corresponding virtual file is stored in file_data.

### hash_data
File containing verification data for the virtual filesystem.

## Functions

### init_fs
Parameters: file_data, directory_table, hash_data, number of processors
* This function initialises data structures required and returns a pointer to a memory area where data is stored to use during filesystem operations.

### close_fs
* This function is called to end all operations on the virtual filesystem.

### create_file
Parameters: file name, file length
* The function creates a file with the given file name and size, and fill it with zero bytes.
* The file is placed at the smallest offset with contiguous space that can fit the size.

### resize_file
Parameters: file name, new length
* This resizes the given file name to the specified new length.
* If the new size does not fit the current space the file occupies, repack is called, then the filesystem will attempt to find the first contiguous space.
* If increasing file size, then the new space is filled with zero bytes. If decreasing, the end of the file is truncated.

### repack
* When called, the function moves the offsets of all files to the left most possible space with no unallocated spaced in between them.
* File order is unchanged.

### delete_file
Parameters: file name
* Deletes a file with given file name.

### rename_file
Parameters: old name, new name
* Renames a specified file with a new name.

### read_file
Parameters: file name, offset, read length
* Read a specified length from a file from a specified offset.

### write_file
Parameters: file name, offset, write length, input
* Write to a file with the given file name at the specified offset from the start of the file.
* Automatically accomodates for a file that is too small, i.e. repacking and resizing.

### file_size
Parameters: file name
* Returns the file size of the specified file.

### fletcher
* Computes the fletcher hash of the stored files.

### compute_hash_tree
* Computes the entire Merkle hash tree for file_data and stores it in hash_data.

### compute_hash_block
Parameters: offset
* Computes the hash for the block at the given offset in file_data.
