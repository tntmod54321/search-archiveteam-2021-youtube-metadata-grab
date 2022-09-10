# search archiveteam 2021 youtube metadata grab
Search jopik/archiveteam's december 2021 youtube metadata grab without decompressing the zstd files to disk
# NO SUPPORT NO WARRANTY ETC

### use --help
tested on windows 10 with python 3.9

it sure would be nice if this ran multicore (with my 80+ regex searches it was very heavily cpu bottlenecked)... I wasted 2 days trying to do that.  
PRs welcome.  
  
needed improvements:  
* mutlicore regexing (search multiple lines at once?)  
* log the amount of lines read so far (just save 'L'+previous amount when you save the management file)
