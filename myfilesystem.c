#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <pthread.h>

#include "myfilesystem.h"

typedef struct {
	int offset;
	int length;
} pair;

// Global file pointers
FILE* file1;
FILE* file2;
FILE* file3;

// Single global mutex
pthread_mutex_t masterLock = PTHREAD_MUTEX_INITIALIZER;

// Update files writing from virtual helper to actual file
// Which file to update is specified
void update(int update_file, int update_dir, int update_hash, void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	if (update_file == 1) {
		rewind(file1);
		fwrite(temp->file_data, temp->file_size, 1, file1);
		fflush(file1);
	}
	
	if (update_dir == 1) {
		rewind(file2);
		fwrite(temp->directory, temp->dir_size, 1, file2);
		fflush(file2);
	}
	
	if (update_hash == 1) {
		rewind(file3);
		fwrite(temp->hash_data, temp->hash_size, 1, file3);
		fflush(file3);
	}
}

// Compare function for qsort
int compare(const void* a, const void* b) {
	pair x = *((pair*) a);
	pair y = *((pair*) b);
	
	if (x.offset == y.offset) return 0;
	else if (x.offset < y.offset) return -1;
	else return 1;
}

// Get offset of index i in directory_table of helper
int get_offset(int i, void* helper) {
	struct helper* temp = (struct helper*) helper;
	int offset;
	int index = i * 72 + 64;
	memcpy(&offset, temp->directory+index, sizeof(int));
	return offset;
}

// Get length of index i in directory_table of helper
int get_length(int i, void* helper) {
	struct helper* temp = (struct helper*) helper;
	int length;
	int index = i * 72 + 68;
	memcpy(&length, temp->directory+index, sizeof(int));
	return length;
}

// Get file_data contents of index i in directory_table (free after use)
char* get_contents(int i, void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	int offset = get_offset(i, temp);
	int length = get_length(i, temp);
	
	char* data = temp->file_data;
	char* buf = malloc((length + 1) * sizeof(char));
	
	memcpy(buf, &data[offset], length);
	buf[length] = '\0';
	
	return buf;
}

// Write length bytes to file_data at offset
void write_contents(int offset, int length, void* buf, void* helper) {
	struct helper* temp = (struct helper*) helper;
	memcpy(temp->file_data+offset, buf, length);
	return;
}

void zero_out(int offset, int length, void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	char null[length];
	for (int i = 0; i < length; ++i) {
		null[i] = 0;
	}
	write_contents(offset, length, null, temp);
}

// Write to index i of the directory_table with given filename, offset and length
void write_directory(int i, char* filename, int offset, int length, int clear, void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	if (strlen(filename) < 64) {
		
	}
	
	memcpy(temp->directory+i*72, filename, 64);
	memcpy(temp->directory+i*72+64, &offset, 4);
	memcpy(temp->directory+i*72+68, &length, 4);
	
	// Zero it out if wanted
	if (clear == 1) {
		zero_out(offset, length, temp);
	}
}

// Make array of ascending pairs of offset and length, return 1 if file exists
void sort_dir(pair* files, int max, void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	for (int i = 0; i < max; i++) {		
		pair current;
		
		current.offset = i;
		current.length = -1;
		
		if (strcmp(temp->directory+i*72, "") != 0) {
			current.offset = get_offset(i, temp);
			current.length = get_length(i, temp);
		}
		
		files[i] = current;
	}
	
	qsort(files, max, sizeof(pair), compare);
}

// Returns offset of first free directory, otherwise -1
int get_first_free(pair* files, int max) {
	for (int i = 0; i < max; ++i) {
		if (files[i].length == -1) {
			return files[i].offset;
		}
	}
	return -1;
}

// Check if there's space available to repack where space is total space
int check_unused(pair* files, int max, int space) {
	int count = 0;
	for (int i = 0; i < max; ++i) {
		if (files[i].length >= 0) {
			count += files[i].length;
		}
	}
	return space - count;
}

// Print out data structure (for debugging)
void print(void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	printf("### FILE CONTENTS ###\n\n");
	for (int i = 0; i < temp->dir_size/72; ++i) {
		char* contents = get_contents(i, temp);
		printf("%d: %s\t%d\t%d\n", i+1, temp->directory+i*72, get_offset(i, temp), get_length(i, temp));
		printf("Contents: %s\n\n", contents);
		free(contents);
	}
	
	printf("### HASH DATA ###\n\n");
	for (int i = 0; i < temp->hash_size/16; ++i) {
		uint8_t hash[16];
		memcpy(hash, temp->hash_data + i * 16, 16);
		
		printf("Hash [%d] : ", i);
		for (int j = 0; j < 16; ++j) {
			printf("%d\t", hash[j]);
		}
		printf("\n");
	}
}

void repack_helper(void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	int max = temp->dir_size/72;
	int offset = 0;
	
	pair files[max];
	sort_dir(files, max, temp);
	
	// For all valid files (length not -1)
	for (int i = 0; i < max; ++i) {
		if (files[i].length >= 0) {
			
			// Get the index of file with this offset
			for (int j = 0; j < max; ++j) {
				if (get_offset(j, temp) == files[i].offset) {
					
					// Set offset to new offset and move file_data accordingly
					char* contents = get_contents(j, temp);
					write_directory(j, temp->directory+j*72, offset, get_length(j, temp), 1, temp);
					write_contents(offset, get_length(j, temp), contents, temp);
					free(contents);
					break;
				}
			}
			
			// The next file should be right after this one
			offset += files[i].length;
		}
	}
	
	// Zero out everything after that
	zero_out(offset, temp->file_size - offset, temp);
	update(1, 1, 0, temp);
	compute_hash_tree(temp);
}

// Verify hash of block is correct
int verify_hash_block(int block_offset, void* helper) {
	struct helper* temp = (struct helper*) helper;

	// Calculate index of block_offset in hash_table
	int start = temp->file_size / 256 - 1;
	int index = start + block_offset;
	
	// Initialise holders
	uint8_t hash_old[16];
	uint8_t hash_new[16];
	uint8_t contents[256];
	
	// Compute hash all the way up to the root (assume root is correct)
	while (index > 0) {
		
		// Initialise current node and old hash to check
		int cur = index * 16;
		memcpy(hash_old, temp->hash_data + cur, 16);
		
		// If it's a leaf
		if (index >= start) {

			// Copy block from file_data
			memcpy(contents, temp->file_data + block_offset * 256, 256);

			// Calculate new hash
			fletcher(contents, 256, hash_new);

		// Otherwise a non-leaf node
		} else {

			// Left and right children
			int left = (index * 2 + 1) * 16;
			int right = (index * 2 + 2) * 16;
			
			// Copy left and right hash children
			memcpy(contents, temp->hash_data + left, 16);
			memcpy(contents + 16, temp->hash_data + right, 16);

			// Calculate hash for that and store at hash_data[j]
			fletcher(contents, 32, hash_new);
		}
		
		if (memcmp(hash_old, hash_new, 16) != 0) {
			return 3;
		}

		// Must be left child
		if (index % 2 != 0) {
			index = (index - 1) / 2;

		// Otherwise right child
		} else {
			index = (index - 2) / 2;
		}
	}
	
	return 0;
}

// Method to recalculate hash blocks the file is in
void compute_file_hash(int i, void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	int cur = get_offset(i, temp);
	
	while (cur < get_offset(i, temp) + get_length(i, temp)) {
		int block_offset = cur/256;
		compute_hash_block(block_offset, temp);
		cur += 256;
	}
}

void * init_fs(char * f1, char * f2, char * f3, int n_processors) {
	
	// If they don't exist, return NULL
	if (fopen(f1, "rb+") == NULL || fopen(f2, "rb+") == NULL || fopen(f3, "rb+") == NULL) {
		printf("File does not exist.\n");
		return NULL;
	}
	
	// Read file_data
	file1 = fopen(f1, "rb+");
	struct helper* result = malloc(sizeof(struct helper));
	
	// Get length of file
	fseek(file1, 0, SEEK_END);
	result->file_size = (int) ftell(file1);
	rewind(file1);
	
	// Write any content into the helper struct
	result->file_data = malloc(result->file_size * sizeof(char));
	fread(result->file_data, result->file_size, 1, file1);
	
	// Do the same for directory_table
	file2 = fopen(f2, "rb+");
	fseek(file2, 0, SEEK_END);
	result->dir_size = (int) ftell(file2);
	rewind(file2);
	
	result->directory = malloc(result->dir_size * sizeof(char));
	fread(result->directory, result->dir_size, 1, file2);
	
	// Open hash_data
	file3 = fopen(f3, "rb+");
	result->hash_size = 16 * ((2 * (result->file_size / 256)) - 1);
	result->hash_data = malloc(result->hash_size * sizeof(char));
	fread(result->hash_data, result->hash_size, 1, file3);
	//print(result);
    return result;
}

void close_fs(void * helper) {
	struct helper* temp = (struct helper*) helper;
	//print(temp);
	free(temp->file_data);
	free(temp->directory);
	free(temp->hash_data);
	free(helper);
	
	fclose(file1);
	fclose(file2);
	fclose(file3);
}

int create_helper(char* filename, size_t length, void* helper) {
	struct helper* temp = (struct helper*) helper;
	
	int max = temp->dir_size/72;
	int offset = 0;
	
	// If filename already exists, return 1
	for (int i = 0; i < max; ++i) {
		if (strcmp(temp->directory+i*72, filename) == 0) return 1;
	}
	
	// Make array of ascending pairs of offset and length
	pair files[max];
	sort_dir(files, max, temp);
	
	// Get first free directory and return 2 if not enough space
	int free = get_first_free(files, max);
	if (free == -1) return 2;
	
	// For all valid files (length not -1)
	for (int i = 0; i < max; ++i) {
		if (files[i].length >= 0) {
			
			// If previous doesn't collide with next one, all good
			if (offset + length <= files[i].offset) {
				write_directory(free, filename, offset, length, 1, temp);
				update(1, 1, 0, temp);
				compute_hash_tree(temp);
				return 0;
			}
			
			// Otherwise, it collides so check end of next file
			offset = files[i].offset + files[i].length;
		}
	}
	
	// If offset is still 0, there are no existing files yet (1) or
	// If we can append after the last file in file_data (2)
	if ((offset == 0 && offset + length <= temp->file_size) || offset + length <= temp->file_size) {
		write_directory(free, filename, offset, length, 1, temp);
		update(1, 1, 0, temp);
		compute_hash_tree(temp);
		return 0;
		
	// Otherwise no currently available space
	} else {
		
		// If there is still space for the new file, repack
		if (check_unused(files, max, temp->file_size) >= length) {
			repack_helper(temp);
			return create_helper(filename, length, temp);
		}
		
		// Otherwise no more space
		return 2;
	}
}

int create_file(char * filename, size_t length, void * helper) {
	pthread_mutex_lock(&masterLock);
	int ret = create_helper(filename, length, helper);
	pthread_mutex_unlock(&masterLock);
	return ret;
}

int resize_helper(char* filename, size_t length, void* helper) {
	struct helper* temp = (struct helper*) helper;
	int max = temp->dir_size/72;
	
	// Search for filename in directory_table
	for (int i = 0; i < max; i++) {
		if (strcmp(temp->directory+i*72, filename) == 0) {
			
			// If the length is less than it already is, truncate it
			if (length <= get_length(i, temp)) {
				write_directory(i, filename, get_offset(i, temp), length, 0, temp);
				update(0, 1, 0, temp);
				return 0;
			}
			
			int offset = get_offset(i, temp);
			pair files[max];
			sort_dir(files, max, temp);
			
			// For all valid files (length not -1)
			for (int j = 0; j < max; ++j) {
				if (files[j].length >= 0) {
					
					// If they collide with the file we want to resize
					if (offset < files[j].offset) {
						 if (offset + length > files[j].offset) {
							 
							 // Check if we can repack to fit the new file
							 int old = get_length(i, temp);
							 int unused = check_unused(files, max, temp->file_size) + old;
							 
							 // If there's space available after repack
							 if (unused >= length) {
								 
								 // Delete file, repack, then put it back in
								 char* contents = get_contents(i, temp);
								 delete_file(filename, temp);
								 repack_helper(temp);
								 write_directory(i, filename, temp->file_size - unused, length, 1, temp);
								 write_contents(temp->file_size - unused, old, contents, temp);
								 free(contents);
								 
								 update(1, 1, 0, temp);
								 compute_hash_tree(temp);
								 return 0;
							 }
							 
							 // No space at all, so return 2
							 return 2;
						 }					
					}
				}
			}
			
			// It doesn't collide with anything, so check if it collides with end
			if (offset + length <= temp->file_size) {
				write_directory(i, filename, offset, length, 0, temp);
				update(0, 1, 0, temp);
				return 0;
				
			// Otherwise there's not enough room
			} else {
				
				// Check if we can repack to fit the new file
				int unused = check_unused(files, max, temp->file_size) + get_length(i, temp);

				// If there's space available after repack
				if (unused >= length) {
					
					// Because it's at the end simply repack and resize
					repack_helper(temp);
					return resize_helper(filename, length, temp);
				}

				// No space left, so return 2
				return 2;
			}
		}
	}
	
	// Return 1 if file does not exist or error
	return 1;
}

int resize_file(char * filename, size_t length, void * helper) {
	pthread_mutex_lock(&masterLock);
	int ret = resize_helper(filename, length, helper);
	pthread_mutex_unlock(&masterLock);
	return ret;
}

void repack(void * helper) {
	pthread_mutex_lock(&masterLock);
	repack_helper(helper);
	pthread_mutex_unlock(&masterLock);
}

int delete_file(char * filename, void * helper) {
    struct helper* temp = (struct helper*) helper;
	
	// Search for filename in directory_table
	for (int i = 0; i < temp->dir_size/72; i++) {
		if (strcmp(temp->directory+i*72, filename) == 0) {
			char null[64];
			null[0] = '\0';
			
			write_directory(i, null, 0, 0, 0, temp);
			update(0, 1, 0, temp);
			return 0;
		}
	}
	
	// Return 1 if file does not exist or error
	return 1;
}

int rename_file(char * oldname, char * newname, void * helper) {
    struct helper* temp = (struct helper*) helper;
	
	// Search for oldname in directory_table
	for (int i = 0; i < temp->dir_size/72; i++) {
		if (strcmp(temp->directory+i*72, oldname) == 0) {
			
			// Check if newname exists already
			for (int i = 0; i < temp->dir_size/72; i++) {
				if (strcmp(temp->directory+i*72, newname) == 0) return 1;
			}
			
			write_directory(i, newname, get_offset(i, temp), get_length(i, temp), 0, temp);
			update(0, 1, 0, temp);
			return 0;
		}
	}
	
	// Return 1 if file does not exist or error
	return 1;
}

int read_file(char * filename, size_t offset, size_t count, void * buf, void * helper) {
	struct helper* temp = (struct helper*) helper;
	
	// Search for filename in directory_table
	for (int i = 0; i < temp->dir_size/72; i++) {
		if (strcmp(temp->directory+i*72, filename) == 0) {
			
			// If the end index of what you want to read exceeds the length of file
			if (offset + count > get_length(i, temp)) return 2;
			
			// Otherwise it means it's valid, so reeeeeeead it
			char* contents = get_contents(i, temp);
			memcpy(buf, contents + offset, count);
			free(contents);
			
			// Verify hash
			int cur = get_offset(i, temp);
			while (cur < get_offset(i, temp) + get_length(i, temp)) {
				int block_offset = cur/256;
				if (verify_hash_block(block_offset, temp) == 3) return 3;
				cur += 256;
			}
			
			return 0;
		}
	}
	
	// Return 1 if file does not exist
    return 1;
}

int write_helper(char * filename, size_t offset, size_t count, void * buf, void * helper) {
	struct helper* temp = (struct helper*) helper;
	
	// Search for filename in directory_table
	for (int i = 0; i < temp->dir_size/72; i++) {
		if (strcmp(temp->directory+i*72, filename) == 0) {
			
			// If offset is greater than the current size of the file
			if (offset > get_length(i, temp)) return 2;
			
			// If writing will exceed the file size
			if (offset + count > get_length(i, temp)) {
				
				// Attempt resizing the file
				int ret = resize_helper(filename, offset + count, temp);
				
				// If there is no space after attempting resize, return 3
				if (ret == 2) return 3;
			}
			
			// Otherwise it's within bounds so write away
			write_contents(get_offset(i, temp) + offset, count, buf, temp);
			update(1, 1, 0, temp);
			compute_hash_tree(temp);
			return 0;
		}
	}
	
	// Return 1 if file does not exist
    return 1;
}

int write_file(char * filename, size_t offset, size_t count, void * buf, void * helper) {
	pthread_mutex_lock(&masterLock);
	int ret = write_helper(filename, offset, count, buf, helper);
	pthread_mutex_unlock(&masterLock);
    return ret;
}

ssize_t file_size(char * filename, void * helper) {
	struct helper* temp = (struct helper*) helper;
	
	// Search for filename in directory_table
	for (int i = 0; i < temp->dir_size/72; i++) {
		if (strcmp(temp->directory+i*72, filename) == 0) return get_length(i, temp);
	}
	
	// Return -1 if file does not exist
	return -1;
}

void fletcher(uint8_t * buf, size_t length, uint8_t * output) {
	
	// Make sure it's a multiple of four
	size_t new = length;
	if (new % 4 != 0) {
		new += 4 - (new % 4);
	}
	
	// Copy and pad 0's if necessary
	uint8_t temp[new];
	memcpy(temp, buf, length);
	
	for (int i = length; i < new; ++i) {
		temp[i] = 0;
	}
	
	// Read as 4 byte blocks
	uint32_t* read = (uint32_t*) temp;
	
	// Fletcher hash
	uint64_t a = 0, b = 0, c = 0, d = 0;
	uint64_t power = (uint64_t) (pow(2, 32) - 1);
	
	for (int i = 0; i < new/4; ++i) {
		a = (a + read[i]) % power;
		b = (b + a) % power;
		c = (c + b) % power;
		d = (d + c) % power;
	}
	
	// Treat them as uint32_t then copy to output
	uint32_t a_four = (uint32_t) a;
	uint32_t b_four = (uint32_t) b;
	uint32_t c_four = (uint32_t) c;
	uint32_t d_four = (uint32_t) d;
	
	memcpy(output, &a_four, 4);
	memcpy(output+4, &b_four, 4);
	memcpy(output+8, &c_four, 4);
	memcpy(output+12, &d_four, 4);
}

void compute_hash_tree(void * helper) {
	struct helper* temp = (struct helper*) helper;
	
	// Calculate number of levels
	int levels = log2(temp->file_size / 256);
	
	// Initialise holders
	uint8_t hash[16];
	uint8_t contents[256];
	
	// For every level from bottom up
	for (int i = levels; i >= 0; i--) {
		
		// Every level has start to end inclusive
		uint64_t start = pow(2, i) - 1;
		uint64_t end = pow(2, i+1) - 2;
		
		// For every node on that level
		for (uint64_t j = start; j <= end; ++j) {
			
			// Initialise current node
			int cur = j * 16;
			
			// If they're leaves
			if (i == levels) {
				
				// Copy block from file_data
				int block_offset = j - start;
				memcpy(contents, temp->file_data + block_offset * 256, 256);
				
				// Calculate hash and store at hash_data[j]
				fletcher(contents, 256, hash);
				memcpy(temp->hash_data + cur, hash, 16);
			
			// Otherwise they're non-leaf nodes
			} else {
				
				// Left and right children
				int left = (j * 2 + 1) * 16;
				int right = (j * 2 + 2) * 16;
				
				// Copy left and right hash children
				memcpy(contents, temp->hash_data + left, 16);
				memcpy(contents + 16, temp->hash_data + right, 16);
				
				// Calculate hash for that and store at hash_data[j]
				fletcher(contents, 32, hash);
				memcpy(temp->hash_data + cur, hash, 16);
			}
		}
	}
	
	update(0, 0, 1, temp);
}

void compute_hash_block(size_t block_offset, void * helper) {
	struct helper* temp = (struct helper*) helper;

	// Calculate index of block_offset in hash_table
	int start = temp->file_size / 256 - 1;
	int index = start + block_offset;
	
	// Initialise holders
	uint8_t hash[16];
	uint8_t contents[256];
	
	// Compute hash all the way up to the root
	while (index >= 0) {
		
		// Initialise current node and block offset
		int cur = index * 16;
		
		// If it's a leaf
		if (index >= start) {

			// Copy block from file_data
			memcpy(contents, temp->file_data + block_offset * 256, 256);

			// Calculate hash and store at hash_data[j]
			fletcher(contents, 256, hash);
			memcpy(temp->hash_data + cur, hash, 16);

		// Otherwise a non-leaf node
		} else {

			// Left and right children
			int left = (index * 2 + 1) * 16;
			int right = (index * 2 + 2) * 16;
			
			// Copy left and right hash children
			memcpy(contents, temp->hash_data + left, 16);
			memcpy(contents + 16, temp->hash_data + right, 16);

			// Calculate hash for that and store at hash_data[j]
			fletcher(contents, 32, hash);
			memcpy(temp->hash_data + cur, hash, 16);
		}

		// Must be left child
		if (index % 2 != 0) {
			index = (index - 1) / 2;

		// Otherwise right child
		} else {
			index = (index - 2) / 2;
		}
	}
	
	update(0, 0, 1, temp);
}
