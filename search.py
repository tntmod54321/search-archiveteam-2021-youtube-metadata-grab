import re
import sys
import json
import zstandard as zstd
import io
import time
from os import listdir, makedirs
from os.path import isfile, isdir, splitext, split, join
import concurrent.futures
from concurrent.futures import *
import traceback
import uuid
import os

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

def writemsg(msg):
	sys.stdout.write(msg)
	sys.stdout.flush()

def searchline(string, expression):
	result=False
	if expression.search(string, re.IGNORECASE): result=True
	return result

def worker2(line, expressions):
	# print("worker function called!!")
	
	string = line.decode("utf-8")
	results={}
	for expression in expressions.keys():
		result = searchline(string, expression)
		if result: results[str(uuid.uuid4())] = {"filename": expressions[expression], "binary": line}
		# if result: results.append({"filename": expressions[expression], "binary": line})
	
	return results

def writeResults(results, outputdir):
	keys = list(results.keys()) # try to get a static ver so it doesn't change during iteration ? idk.
	
	file_results={}
	for key in keys: # create file dicts
		file_results[results[key]["filename"]]=[]
	
	for key in keys: # .pop results from results dict
		file_results[results[key]["filename"]].append(results.pop(key)["binary"])
	
	for file in file_results.keys(): # .writelines to disk
		with open(join(outputdir, file), "ab") as f:
			f.writelines(file_results[file])
	
	return

def printHelp():
	print("Usage:", "py search.py -i [INPUT FOLDER] -o [RESULTS FILES FOLDER] -q [QUERY JSON FILE] -m [OUTPUT MANAGEMENT FILE]\n\n"+
	"Arguments:\n", "  -h, --help\t\t\tdisplay this usage info\n",
	"  -i, --input-folder\t\tinput folder containing the .zst metadata files\n",
	"  -o, --output-dir\t\tfolder to output the results files\n",
	"  -q, --query-json\t\tjson file containing your query(ies)\n",
	"  -m, --search-management-file\tfile to keep track of which zst files have been searched")
	exit()

def main():
	files_folder = ""
	query_json = ""
	outputdir = ""
	management_file = ""
	
	if len(sys.argv[1:]) == 0: printHelp()
	i=1 # for arguments like [--command value] get the value after the command
	# first arg in sys.argv is the python file
	for arg in sys.argv[1:]:
		if (arg in ["help", "/?", "-h", "--help"]): printHelp()
		if (arg in ["-o", "--output-dir"]): outputdir = sys.argv[1:][i]
		if (arg in ["-q", "--query-json"]): query_json = sys.argv[1:][i]
		if (arg in ["-i", "--input-folder", "--files-folder"]): files_folder = sys.argv[1:][i]
		if (arg in ["-m", "--search-management-file"]): management_file = sys.argv[1:][i]
		
		i+=1

	if "" in [files_folder, query_json, outputdir, management_file]: printHelp() # these args are critical, print help if not present
	
	zstdfiles = find_files(files_folder, ".zst")
	zstdfiles.sort()
	files_cnt = len(zstdfiles)
	
	### load queries
	with open(query_json, "rb") as f:
		queries={}
		x = json.loads(f.read().decode("utf-8"))
		print("compiling queries")
		for y in x:
			for expression in y["expressions"]:
				expression = re.compile(expression, re.IGNORECASE) # compile query(ies) for extra speed
				queries[expression] = y["filename"]
		results_files = []
		for y in x:
			results_files.append(y["filename"])
	
	# common test query
	# expression = re.compile("BaLl", re.IGNORECASE)
	# queries[expression]="test.json"
	
	### create output folder
	try:
		makedirs(outputdir)
	except FileExistsError:
		pass
	
	### load management file if exists
	if isfile(management_file):
		with open(management_file, "rb") as f:
			m_file = json.loads(f.read().decode("utf-8"))
			completed_files = m_file["c_files"]
			cL = m_file["c_lines"]
	else:
		completed_files = []
		cL=0
	
	### search zstd files
	totallines = 4557605031 # 4.56b
	dctx = zstd.ZstdDecompressor() # reuse decompressor object (docs recommend, woohoo!)
	try:
		for file in zstdfiles: # should remove searched files listed in management file earlier
			if file in completed_files:
				print(f"skipping file {file} (completed)\n")
				continue
			
			writemsg(f"searching {file}:\n")
			now=time.time()
			with open(file, "rb") as f:
				dobj = dctx.stream_reader(f) # stream file
				dbuf = io.BufferedReader(dobj)
				
				L=0
				R=0
				### search file
				results={}
				while True:
					line=dbuf.readline()
					if not line: break
					### search message
					line_results = worker2(line, queries)
					if line_results:
						writemsg('x')
						for result in line_results.keys():
							results[result]=line_results[result]
							R+=1
					L+=1
					if (L/1000).is_integer(): writemsg('.') # could dump results every 1k lines
				writemsg("\n")
			
			### dump found results to disk
			writeResults(results, outputdir)
			
			### update management file
			completed_files.append(file)
			cL += L
			with open(management_file, "wb") as f: # overwrite old one with new one
				f.write(json.dumps({"c_files": completed_files, "c_lines": cL}).encode("utf-8"))
			
			elapsed=time.time()-now
			print(f"({len(completed_files)}/{len(zstdfiles)}) took {round(elapsed, 2)}s to search {L} lines ({round(L/elapsed, 2)}/s), found {R} results ({cL} lines total) ({round((cL/totallines)*100, 2)}% done searching)\n\n")
		print(f"finished searching {len(completed_files)} files")
	except KeyboardInterrupt:
		print("\nInterrupted!")
		os._exit(0)

if __name__ == "__main__":
	main()
