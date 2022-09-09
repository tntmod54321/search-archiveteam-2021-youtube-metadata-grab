import re
import sys
import json
import zstandard as zstd
import io
import time
from os import listdir, makedirs
from os.path import isfile, isdir, splitext, split, join
from multiprocessing.pool import Pool
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
	print("worker online!!")
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

def worker2(line, expressions):
	# print("worker function called!!")
	
	string = line.decode("utf-8")
	results={}
	for expression in expressions.keys():
		result = searchline(string, expression)
		if result: results[str(uuid.uuid4())] = {"filename": expressions[expression], "binary": line}
		# if result: results.append({"filename": expressions[expression], "binary": line})
	
	return results

def worker3():
	print("HI!!")
	return

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

### parse args
# update help message
def printHelp():
	# print sections for different downloaders (programmatically -> _downloaders -> downloader.info["help"]
	# list valid ebay resolutions (leave blank for the max available)
	print("Usage:", "py ebay_archive.py [ID or URL] [OUTPUT DIRECTORY] [ADDITIONAL ARGUMENTS]\n\n"+
	"Arguments:\n", "  -h, --help\t-\tdisplay this usage info")
	exit()

def main():
	global L # remove these ?
	global results
	
	reading_file=False
	shutdownflag=False

	files_folder = ""
	query_json = ""
	outputdir = ""
	management_file = ""
	single_threaded = False
	stream_file = False
	
	if len(sys.argv[1:]) == 0: printHelp()
	i=1 # for arguments like [--command value] get the value after the command
	# first arg in sys.argv is the python file
	for arg in sys.argv[1:]:
		if (arg in ["help", "/?", "-h", "--help"]): printHelp()
		if (arg in ["-o", "--output-dir"]): outputdir = sys.argv[1:][i]
		if (arg in ["-q", "--query-json"]): query_json = sys.argv[1:][i]
		if (arg in ["-i", "--input-folder", "--files-folder"]): files_folder = sys.argv[1:][i]
		if (arg in ["-m", "--search-management-file"]): management_file = sys.argv[1:][i]
		if (arg in ["--single-threaded"]): single_threaded = True # if False then let python decide
		if (arg in ["--stream-files"]): stream_file = True # if False then let python decide
		
		i+=1

	if "" in [files_folder, query_json, outputdir, management_file]: printHelp() # these args are critical, print help if not present
	if single_threaded: print("running single-threaded")

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
	
	# common test query
	queries["ball"]="test.json"
	
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
	dctx = zstd.ZstdDecompressor() # reuse decompressor object (docs recommend, woohoo!)
	try:
		for file in zstdfiles: # should remove searched files listed in management file earlier
			if file in completed_files:
				print(f"skipping file {file} (completed)\n")
				continue
			
			writemsg(f"searching {file}:\n")
			now=time.time()
			with open(file, "rb") as f:
				if stream_file:
					# print("streaming file")
					dobj = dctx.stream_reader(f) # stream file
				else:
					# print("loading entire file")
					dobj = dctx.stream_reader(f.read()) # load file into mem and stream the decoded data
				dbuf = io.BufferedReader(dobj)
				
				L=0
				
				# test speed of multi-threaded streaming vs loading whole file into mem
				# if no significant difference then just switch to streaming
				
				### search file
				if single_threaded:
					results={}
					while True:
						line=dbuf.readline()
						if not line: break
						### search message
						line_results = worker2(line, queries)
						if line_results:
							for result in line_results.keys():
								results[result]=line_results[result]
						L+=1
						# if (L/1000).is_integer(): writemsg('.') # could dump results every 1k lines
						if (L/10000).is_integer():
							writemsg('.') # could dump results every 1k lines
							break
				
				else:
					
					# hmm, assigning multiple expressions for a single line
					# is only useful for running multiple queries.
					# maybe make each thread request a line somehow? idk
					
					# create worker thingy
					with concurrent.futures.ProcessPoolExecutor() as executor:
						# load X amount of lines ?
						
						# lines=[]
						for i in range(0, 5000000):
							line = dbuf.readline()
							if not line: break
							# lines.append(line)
							
						elapsed=time.time()-now
						# print(lines)
						print(elapsed)
						print(i)
						time.sleep(10)
						exit()
						for number, prime in zip(PRIMES, executor.map(is_prime, PRIMES)):
							print('%d is prime: %s' % (number, prime))
					
					
					# do main-thread things
					# if results: writeResults(results, outputdir)
					
					print("balls, multithreaded")
				
				print()
				print(f"{L} lines")
				print(f"{len(results)} results")
				# print(list(results.keys())[0])
				print(results.keys())
				# print(results)
				
				writeResults(results, outputdir)
				
				exit()
			
			print("main process exiting...")
			os._exit(0)
			
			### update management file
			completed_files.append(file)
			with open(management_file, "wb") as f: # overwrite old one with new one
				f.write(json.dumps(completed_files).encode("utf-8"))
			
			sys.stdout.write('\n')
			elapsed=time.time()-now
			print(f"took {elapsed} to search {L} lines ({round(L/elapsed, 2)}/s)")
	except KeyboardInterrupt:
		print("\nInterrupted!")
		os._exit(0)

if __name__ == "__main__":
	main()

# to add:
# better logging (time taken, offset, results count, etc)
# estimated time to completion
