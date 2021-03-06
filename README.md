# Mytextpipe: read and analyze text data

`Mytextpipe` is a warm and cozy package for crafting text corpora: read, clean, 
extract text, statistics and metadata.

Package is in alpha stage, therefore some functionalities are still in development 

## Text corpora
Corpus (plural corpora) is a collection of related documents 
that contain natural language text. A corpus is broken down into 
categories of documents and individual documents. 
Categories are folders and individual documents 
are files stored in a file system on disk. 
 
### Corpus disk structure 

corpus  
&nbsp;| -- readme.md  
&nbsp;| -- corpus-files.csv  
└ -- category 1  
&nbsp;|&nbsp;&nbsp;&nbsp;&nbsp;| -- file 1  
&nbsp;|&nbsp;&nbsp;&nbsp;└ -- file 2  
...  
└ -- category n  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;| -- file 1  
&nbsp;
&nbsp;&nbsp;&nbsp;&nbsp;└ -- file n

### Usage example

```python
from mytextpipe import corpus
CORPUS_ROOT = '/home/my/txt_corpus'
corp = corpus.FileCorpusReader(CORPUS_ROOT)
print(corp.stat())
# Outputs corpus statistics
{'files': 35, 'categories': 12, 'ext': {'txt': 10, 'html': 25}, 'file_size': 191219, 'words': 37542}
```

### Class FileCorpusReader
Handle corpus with arbitrary files 

### Class TxtCorpusReader
Handle text files corpus

### Class HTMLCorpusReader
Handle html files corpus

### Class CorpusTransformer
Convert one corpus to another 