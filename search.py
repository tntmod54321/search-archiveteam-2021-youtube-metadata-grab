if __name__ != "__main__":
	print("not running as main, exiting...")
	exit()

import re
import sys
import json
import zstandard as zstd
import io
import time

files_folder = ""
query_json = ""
outputdir = ""
file_offset = 0

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

# Reusing a ZstdCompressor or ZstdDecompressor instance for multiple operations is
# faster than instantiating a new ZstdCompressor or ZstdDecompressor for each operation

file = sys.argv[1]
with io.BytesIO() as dcBuffer:
	now = time.time()
	
	with open(file, "rb") as f:
		dctx = zstd.ZstdDecompressor()
		dctx.copy_stream(f, dcBuffer, read_size=8192, write_size=16384) # streaming method instead of one-shot
		# dcBuffer.write(dctx.decompress(f.read(), max_output_size=7340032))# 7GB max filesize
	
	dcBuffer.seek(0) # have to do this for each read()-like operation
	for line in dcBuffer.readlines():
		print(json.loads(line)["title"])

exit()
