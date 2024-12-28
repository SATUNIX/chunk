
# Chunk 

AI Knowledge base pre-processor:
Processes data of various sizes and types into uniformly sized text data. 

This repository contains the `chunk` program, which can read files (including Parquet for existing datasets, PDF, DOC, ODT, RTF, CSV, and TXT) in a streaming manner and split them into ~5MB text chunks. Each chunk is hashed (SHA-512) and saved to an output directory or location, ensuring that memory usage is kept minimal even for very large files.
This program is useful for the processing of large datasets and various files into uniform text chunks for creating AI knowledge bases for semantic search.   

---

## Features

- Splits files into ~5MB chunk files named `<sha512>_<timestamp>.txt`.
- Uses OpenSSL for SHA-512 hashing.
- Supports Apache Arrow for Parquet file reading (if installed).
- Utilizes external tools for certain file formats (e.g., `pdftotext`, `doc2txt`, `odt2txt`, `unrtf`).
- Streams data rather than loading entire files into memory.

---

## Requirements

- A C++17 compiler (e.g., `g++`, `gcc-c++`, or similar).
- OpenSSL development libraries (e.g., `libssl-dev` or `openssl-devel`).
- Apache Arrow & Parquet libraries if you want Parquet support.
- External programs for PDF/DOC/ODT/RTF (optional):
  - `pdftotext`  
  - `doc2txt` (or `docx2txt`)  
  - `odt2txt`  
  - `unrtf`  

---

## Installation

You can use the provided `install.sh` script to automate the process of installing dependencies, compiling, and placing the `chunk` executable into `/usr/bin/`. To do so:

```bash
# Make the script executable
chmod +x install_chunk.sh

# Run the script 
./install.sh 
```

This script attempts to detect your Linux distribution via /etc/os-release and then install any needed packages using the distro’s package manager. It compiles chunk.cpp with:

```bash
g++ -std=c++17 chunk.cpp -o chunk -lssl -lcrypto -larrow -lparquet
# Finally, it copies the resulting chunk binary into /usr/bin/.
```

## Usage
Once installed, you can run chunk as follows:

```bash
chunk /path/to/file1.ext /path/to/file2.ext ...
```
### Depending on file type:

PDF: Processed via pdftotext (requires pdftotext installed).
DOC/DOCX: Processed via doc2txt or docx2txt.
ODT: Processed via odt2txt.
RTF: Processed via unrtf.
Parquet: Processed via Apache Arrow C++ if installed.
CSV/TXT: Read directly.
Each file’s data is split into ~5MB chunks under a directory like /tmp/chunked_<timestamp>/ or similarly, depending on the code.
