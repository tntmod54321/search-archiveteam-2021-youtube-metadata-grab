if __name__ != "__main__":
	print("not running as main, exiting...")
	exit()

import re
import sys
import json
import zstandard as zstd
import io
import time
from os import listdir, makedirs
from os.path import isfile, isdir, splitext, split

# you can stream ZST files, right?
# well, if you wanna stream like 15gb_c/100gb_d file into memory,
# you (if you're me) have to do it in chunks, if you do that
# and you're operating with newline-delimited json files,
# is it possible to not load to the newline and therefor
# have the last json be incomplete? (and the next, first one
# almost be incomplete?
# is it possible for a single, large json to fill the whole thing
# and not be parsed at all? eieieiei

# ok so I think it just reads that many bytes at once,
# do while True: dc.readline() instead

# add option to use streaming method and one-shot method. (or just remove the one-shot method lmfao)
# compare speeds
# (you use the streaming zstd api either way but .readlines() is one-shot)

files_folder = ""
query_json = ""
outputdir = ""
management_file = ""
file_offset = 0
stream_files = True

#I'm dumb idk if this works properly for numbers that aren't multiples of 10
stdout_update_interval=1000 # print a '.' every x line searches
# try tweaking these (higher = more faster?)
zstd_readsize=8192
zstd_writesize=16384

zstd_chunkreadsize=131075

# need to compare regexing the line 80 times vs 
# loading as json and regexing some fields

### parse args
def printHelp():
	# print sections for different downloaders (programmatically -> _downloaders -> downloader.info["help"]
	# list valid ebay resolutions (leave blank for the max available)
	print("Usage:", "py ebay_archive.py [ID or URL] [OUTPUT DIRECTORY] [ADDITIONAL ARGUMENTS]\n\n"+
	"Arguments:\n", "  -h, --help\t-\tdisplay this usage info")
	exit()

if len(sys.argv[1:]) == 0: printHelp()
i=1 # for arguments like [--command value] get the value after the command
# first arg in sys.argv is the python file
for arg in sys.argv[1:]:
	if (arg in ["help", "/?", "-h", "--help"]): printHelp()
	if (arg == "--offset"): file_offset = int(sys.argv[1:][i]) # offset is ignored if you use a search management file
	if (arg in ["-o", "--output-dir"]): outputdir = sys.argv[1:][i]
	if (arg in ["-q", "--query-json"]): query_json = sys.argv[1:][i]
	if (arg in ["-i", "--input-folder", "--files-folder"]): files_folder = sys.argv[1:][i]
	if (arg in ["-m", "--search-management-file"]): management_file = sys.argv[1:][i]
	if (arg in ["--one-shot"]): stream_files = False
	
	i+=1

if "" in [files_folder, query_json, outputdir]: printHelp() # these args are critical, print help if not present

### build list of zstd files
def find_files(fdir, fext):
	directories = [fdir]
	files = []
	# traverse directories looking for files
	for directory in directories:
		for f in listdir(directory):
			if isfile(directory+"/"+f): files.append(directory+"/"+f)
			elif isdir(directory+"/"+f): directories.append(directory+"/"+f)
			else: print("you shouldn't be seeing this", directory, f)
	
	files2=[]
	# remove non fext files
	for file in files:
		x, extension = splitext(file)
		if extension.lower() == fext:
			files2.append(file)
	print("found {} {} files in {}".format(len(files2), fext, fdir))
	return files2

zstdfiles = find_files(files_folder, ".zst")
zstdfiles.sort()

### load queries
with open(query_json, "rb") as f:
	queries = json.loads(f.read().decode("utf-8"))
# these aren't gonna work for names if you want to use reg expressions
# you need to give each search an output filename
# or how about results=[] is a list of dicts that are filename: [results]


# find all results in one file
# when done with that file
# do for queries:
# if results then writelines() to filename
# this way it can write to multiple files


### create output folder
try:
	makedirs(outputdir)
except FileExistsError:
	pass

def writemsg(msg):
	sys.stdout.write(msg)
	sys.stdout.flush()

def searchline(string, expression):
	result=False
	
	return result

# 4 benchmarks:
# one-shot vs streaming
# and plain regex vs json then regex

### search zstd files
i=0
i+=file_offset # keep track of offset
dctx = zstd.ZstdDecompressor() # reuse decompressor object
try:
	for file in zstdfiles[file_offset:]:
		results = []
		
		with io.BytesIO() as dcBuffer: # new bytesio obj, idk about safely reusing them
			with open(file, "rb") as f: # does copy_stream load the whole compressed file into mem?
				dctx.copy_stream(f, dcBuffer, read_size=zstd_readsize, write_size=zstd_writesize) # streaming method
			
			writemsg(f"{file} searches:\n") # remove newline later
			
			# replace with
			# while True:
			#     x = dcBuffer.readline()
			#     if not x: break # went through entire file
			dcBuffer.seek(0) # have to do this before each read()-like operation
			l=0
			if stream_files:
				while True:
					line = dcBuffer.readline()
					if not line: break
					l+=1
					if (l/stdout_update_interval).is_integer():
						writemsg('.')
						#exit()
			else:
				lines=dcBuffer.readlines() # streams the whole thing from disk into memory at once
				for line in lines:
					# search line here
					
					l+=1
					if (l/stdout_update_interval).is_integer():
						writemsg('.')
			
			# l is equal to the number of results
			
		sys.stdout.write('\n')
		i+=1
except KeyboardInterrupt:
	if management_file=="": print(f"\nInterrupted! last completed file offset is {i}")
	else: print("\nInterrupted!")

exit()

# func to write query results to disk

# to add:
# run queries, test different methods for speed (regex search all strings, regex actual expressions)
# write query results to disk (query in json file: [{"name": name, "regex": regexstr}])
# better logging (time taken, offset, results count, etc)

# multi thread searcher? 1 manager thread that doles out files to search and X searcher threads
# multithreaded searching would definitely require a map file
# also make the manager thread dump results to disk
