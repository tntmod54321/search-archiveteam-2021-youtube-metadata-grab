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

files_folder = ""
query_json = ""
outputdir = ""
management_file = ""
file_offset = 0

#I'm dumb idk if this works properly for numbers that aren't multiples of 10
stdout_update_interval=1000 # print a '.' every x line searches
threads = 2

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
	if (arg in ["-t", "--threads"]): threads = int(sys.argv[1:][i])
	
	i+=1

if "" in [files_folder, query_json, outputdir]: printHelp() # these args are critical, print help if not present
if threads<2:
	print("2 threads minimum (manager+1 worker)")
	exit()

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

# create physical query results files

# results=[] is a list of dicts that are filename: [results]
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

# benchmark plain regex vs json then regex
def searchline(string, expression):
	result=False
	if re.search(expression, string): result=True
	return result

### search zstd files
i=0
i+=file_offset # keep track of offset
dctx = zstd.ZstdDecompressor() # reuse decompressor object
try:
	for file in zstdfiles[file_offset:]: # get rid of this and instead have a list of searched files
		results = {}
		now = time.time()
		with open(file, "rb") as f: # copy_stream copies the whole thing into memory at once -_-
			dobj = dctx.stream_reader(f.read())
			dbuf = io.BufferedReader(dobj)
			
			for querycat in queries: # generate results dicts
				results[querycat["filename"]] = []
			
			l=0
			writemsg(f"searching {file}:\n") # remove newline later
			while True:
				line = dbuf.readline()
				if not line: break
				
				# search line here
				str_line=line.decode("utf-8")
				for querycat in queries:
					queryfilename=querycat["filename"] # remove this
					for ex in querycat["expressions"]:
						# continue
						# if we match with this line for this expression then
						# append this line to the results for the filename
						# it's assigned to
						if searchline(str_line, ex): results[queryfilename].append(line)
				
				l+=1
				if (l/stdout_update_interval).is_integer():
					writemsg('.')
			# balls
			elapsed=time.time()-now
			print()
			print(results)
			print(elapsed)
		
		# l is equal to the number of lines checked
		
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

# estimated time to completion

# multithread the regex searching
# round-robin assign the threads expressions and pass them each the line
