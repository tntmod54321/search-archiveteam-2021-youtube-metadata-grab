if __name__ != "__main__":
	print("not running as main, exiting...")
	exit()

import re
import sys
import json
import zstandard as zstd
import io
import time
from os import listdir
from os.path import isfile, isdir, splitext, split

files_folder = ""
query_json = ""
outputdir = ""
file_offset = 0

zstd_readsize=8192
zstd_writesize=16384

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
	if (arg == "--offset"): file_offset = int(sys.argv[1:][i])
	if (arg in ["-o", "--output-dir"]): outputdir = sys.argv[1:][i]
	if (arg in ["-q", "--query-json"]): query_json = sys.argv[1:][i]
	if (arg in ["-i", "--input-folder", "--files-folder"]): files_folder = sys.argv[1:][i]
	
	i+=1

# if "" in [files_folder, query_json, outputdir]: printHelp()

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
zstd_filenames=[]
for file in zstdfiles:
	zstd_filenames.append(split(file)[1])
zstd_filenames.sort()

exit()


### search zstd files

file = sys.argv[1]
dctx = zstd.ZstdDecompressor() # reuse decompressor object
with io.BytesIO() as dcBuffer: # new bytesio obj, idk about safely reusing them
	with open(file, "rb") as f:
		dctx.copy_stream(f, dcBuffer, read_size=zstd_readsize, write_size=zstd_writesize) # streaming method
	
	dcBuffer.seek(0) # have to do this before each read()-like operation
	for line in dcBuffer.readlines():
		print(json.loads(line)["title"])

exit()
