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
from os.path import isfile, isdir, splitext, split, join
from multiprocessing.pool import ThreadPool
import concurrent.futures
from concurrent.futures import *
import traceback
import uuid
import os
global reading_file
global shutdownflag
global L
global results

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

def writemsg(msg):
	sys.stdout.write(msg)
	sys.stdout.flush()

def searchline(string, expression):
	result=False
	if re.search(expression, string): result=True
	return result

def worker(tid, fbuffer, expressions):
	global reading_file
	global shutdownflag
	global L
	global results
	
	while True:
		try:
			while True: # get a line to process
				if shutdownflag: exit()
				
				if reading_file: pass
				else:
					reading_file = True
					line = fbuffer.readline()
					reading_file = False
					if not line: return # exit thread if no more file to process
					break
				time.sleep(0.01)
			
			string = line.decode("utf-8")
			
			for expression in expressions.keys():
				result = searchline(string, expression)
				if result: results[str(uuid.uuid4())] = {"filename": expressions[expression], "binary": line}
			L+=1
		except:
			with open("errors", "a+") as f:
				f.write(str(traceback.format_exc())+"\n") # traceback thing
			shutdownflag=True
			exit() # KILLLL
	
	return

def writeResults(results, outputdir):
	keys = list(results.keys())
	
	file_results={}
	for key in keys: # create file dicts
		file_results[results[key]["filename"]]=[]
	
	for key in keys: # .pop results from results dict
		file_results[results[key]["filename"]].append(results.pop(key)["binary"])
	
	for file in file_results.keys(): # .writelines to disk
		with open(join(outputdir, file), "ab") as f:
			f.writelines(file_results[file])
	
	return

reading_file=False
shutdownflag=False

files_folder = ""
query_json = ""
outputdir = ""
management_file = ""
workerthreads = 1 # change default to 2 or 4

### parse args
# update help message
def printHelp():
	# print sections for different downloaders (programmatically -> _downloaders -> downloader.info["help"]
	# list valid ebay resolutions (leave blank for the max available)
	print("Usage:", "py ebay_archive.py [ID or URL] [OUTPUT DIRECTORY] [ADDITIONAL ARGUMENTS]\n\n"+
	"Arguments:\n", "  -h, --help\t-\tdisplay this usage info")
	exit()

def main():
	global reading_file
	global shutdownflag
	global L
	global results
	
	if len(sys.argv[1:]) == 0: printHelp()
	i=1 # for arguments like [--command value] get the value after the command
	# first arg in sys.argv is the python file
	for arg in sys.argv[1:]:
		if (arg in ["help", "/?", "-h", "--help"]): printHelp()
		if (arg in ["-o", "--output-dir"]): outputdir = sys.argv[1:][i]
		if (arg in ["-q", "--query-json"]): query_json = sys.argv[1:][i]
		if (arg in ["-i", "--input-folder", "--files-folder"]): files_folder = sys.argv[1:][i]
		if (arg in ["-m", "--search-management-file"]): management_file = sys.argv[1:][i]
		if (arg in ["-t", "--threads"]): workerthreads = int(sys.argv[1:][i])
		
		i+=1

	if "" in [files_folder, query_json, outputdir, management_file]: printHelp() # these args are critical, print help if not present
	if workerthreads<1:
		print("1 thread minimum")
		exit()

	zstdfiles = find_files(files_folder, ".zst")
	zstdfiles.sort()

	### load queries
	with open(query_json, "rb") as f:
		queries={}
		x = json.loads(f.read().decode("utf-8"))
		for y in x:
			for expression in y["expressions"]:
				queries[expression] = y["filename"]
		results_files = []
		for y in x:
			results_files.append(y["filename"])

	### create output folder
	try:
		makedirs(outputdir)
	except FileExistsError:
		pass

	### load management file if exists
	if isfile(management_file):
		with open(management_file, "rb") as f:
			completed_files = json.loads(f.read().decode("utf-8"))
	else:
		completed_files = []

	### search zstd files
	executor = ThreadPoolExecutor(max_workers=16) # will limit the amount of threads
	dctx = zstd.ZstdDecompressor() # reuse decompressor object (docs recommend, woohoo!)
	try:
		for file in zstdfiles: # should remove searched files listed in management file earlier
			if file in completed_files:
				print(f"skipping file {file} (completed)")
				continue
			
			writemsg(f"searching {file}:\n")
			with open(file, "rb") as f:
				dobj = dctx.stream_reader(f.read())
				dbuf = io.BufferedReader(dobj)
				now=time.time()
				L=0
				futures=[]
				results={}
				reading_file = False
				threads_dead=False
				for i in range(0, workerthreads):
					future = executor.submit(worker, i, dbuf, queries)
					futures.append(future)
				
				# wait for workers to die
				while True:
					if shutdownflag:
						print("thread requested shutdown")
						os._exit(0)
					
					if results: writeResults(results, outputdir) # make sure to dump all results to disk before moving on to next file
					
					future_statuses=[]
					for future in futures:
						future_statuses.append(future.done())
					
					if threads_dead: break # leave while loop after flushing results
					
					# check if all threads are dead
					if all(future_statuses): # we do another loop to make sure we flush the results to disk
						threads_dead=True
						continue
					
					# time.sleep(0.01)
					writemsg('.')
					time.sleep(0.1)
					# time.sleep(5)
			
			### update management file
			completed_files.append(file)
			with open(management_file, "wb") as f:
				f.write(json.dumps(completed_files).encode("utf-8"))
			
			sys.stdout.write('\n')
			elapsed=time.time()-now
			print(f"took {elapsed} to search {L} lines ({round(L/elapsed, 2)}/s)")
	except KeyboardInterrupt:
		print("\nInterrupted!")
		os._exit(0)

main()

# to add:
# better logging (time taken, offset, results count, etc)
# estimated time to completion
