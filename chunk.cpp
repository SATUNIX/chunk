#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include <stdexcept>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/reader.h>

// OpenSSL for SHA-512 (install libssl-dev on Linux)
#include <openssl/evp.h>
#include <openssl/sha.h>

// -----------------------------------------------------------------------------
// 1) Constants
// -----------------------------------------------------------------------------

// ~5MB chunk (5 * 1024 * 1024) plus ~5KB variance
static const size_t CHUNK_BASE_SIZE = 5 * 1024 * 1024; // 5 MB
static const size_t CHUNK_VARIANCE  = 5 * 1024;        // 5 KB
static const size_t CHUNK_LIMIT     = CHUNK_BASE_SIZE + CHUNK_VARIANCE;

// Suppose we allow up to ~500 MB total => 100 buffers * ~5MB each
static const size_t NUM_BUFFERS     = 100;

// We'll store our output directory globally once it's created
static std::string g_outputDir;

// -----------------------------------------------------------------------------
// 2) Compute SHA-512 for a chunk of data
// -----------------------------------------------------------------------------
std::string compute_sha512(const char* data, size_t length) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        throw std::runtime_error("Failed to create EVP_MD_CTX");
    }
    if (EVP_DigestInit_ex(ctx, EVP_sha512(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("Failed to init SHA-512");
    }
    if (EVP_DigestUpdate(ctx, data, length) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("Failed to update SHA-512");
    }

    unsigned char hash[EVP_MAX_MD_SIZE];
    unsigned int hashLen = 0;
    if (EVP_DigestFinal_ex(ctx, hash, &hashLen) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("Failed to finalize SHA-512");
    }
    EVP_MD_CTX_free(ctx);

    std::ostringstream oss;
    for (unsigned int i = 0; i < hashLen; ++i) {
        oss << std::hex << std::nouppercase
        << ((hash[i] & 0xF0) >> 4)
        << (hash[i] & 0x0F);
    }
    return oss.str();
}

// -----------------------------------------------------------------------------
// 3) Data structure: A single chunk buffer
// -----------------------------------------------------------------------------
struct ChunkBuffer {
    std::unique_ptr<char[]> data;
    size_t capacity;
    size_t used;

    ChunkBuffer(size_t cap) : data(new char[cap]), capacity(cap), used(0) {}

    void clear() { used = 0; }
};

// -----------------------------------------------------------------------------
// 4) BufferPool - manages a pool of chunk buffers
// -----------------------------------------------------------------------------
class BufferPool {
public:
    BufferPool(size_t bufferCount, size_t bufferSize) : bufferSize_(bufferSize) {
        for (size_t i = 0; i < bufferCount; i++) {
            freeBuffers_.emplace_back(std::make_unique<ChunkBuffer>(bufferSize_));
        }
    }

    std::unique_ptr<ChunkBuffer> acquireBuffer() {
        if (freeBuffers_.empty()) {
            throw std::runtime_error("No free chunk buffers available!");
        }
        auto buf = std::move(freeBuffers_.back());
        freeBuffers_.pop_back();
        buf->clear();
        return buf;
    }

    void releaseBuffer(std::unique_ptr<ChunkBuffer> buf) {
        buf->clear();
        freeBuffers_.push_back(std::move(buf));
    }

private:
    size_t bufferSize_;
    std::vector<std::unique_ptr<ChunkBuffer>> freeBuffers_;
};

// -----------------------------------------------------------------------------
// 5) Chunker - streams data into ~5MB chunks, writes them out to disk
// -----------------------------------------------------------------------------
class Chunker {
public:
    Chunker(BufferPool& pool, const std::string& outputDir)
    : pool_(pool), outputDir_(outputDir)
    {
        currentBuffer_ = pool_.acquireBuffer();
    }

    ~Chunker() {
        // flush leftover data on destruction
        flushCurrentBuffer();
    }

    // push data into the chunker, splitting across multiple ~5MB chunks if needed
    void pushData(const char* data, size_t len) {
        size_t offset = 0;
        while (offset < len) {
            size_t spaceLeft = CHUNK_LIMIT - currentBuffer_->used;
            if (spaceLeft == 0) {
                // current buffer is full
                flushCurrentBuffer();
                currentBuffer_ = pool_.acquireBuffer();
                continue;
            }

            size_t toWrite = std::min(spaceLeft, len - offset);
            std::memcpy(currentBuffer_->data.get() + currentBuffer_->used,
                        data + offset, toWrite);
            currentBuffer_->used += toWrite;
            offset += toWrite;

            if (currentBuffer_->used >= CHUNK_LIMIT) {
                flushCurrentBuffer();
                currentBuffer_ = pool_.acquireBuffer();
            }
        }
    }

private:
    BufferPool& pool_;
    std::string outputDir_;
    std::unique_ptr<ChunkBuffer> currentBuffer_;

    // flushCurrentBuffer - finalize chunk, compute SHA-512, write to disk
    void flushCurrentBuffer() {
        if (!currentBuffer_ || currentBuffer_->used == 0) {
            return;
        }
        const char* dataPtr = currentBuffer_->data.get();
        size_t dataLen      = currentBuffer_->used;
        // compute hash
        std::string hashVal = compute_sha512(dataPtr, dataLen);

        // build output filename
        std::time_t now = std::time(nullptr);
        std::ostringstream fname;
        fname << outputDir_ << "/" << hashVal << "_" << now << ".txt";

        // write chunk to file
        std::ofstream ofs(fname.str(), std::ios::binary);
        if (!ofs.is_open()) {
            std::cerr << "Error writing chunk file: " << fname.str() << "\n";
        } else {
            ofs.write(dataPtr, dataLen);
            ofs.close();
            std::cout << "Flushed chunk -> " << fname.str()
            << " (size: " << dataLen << " bytes)\n";
        }

        // release buffer
        pool_.releaseBuffer(std::move(currentBuffer_));
    }
};

// -----------------------------------------------------------------------------
// 6) Streaming file read helper
// -----------------------------------------------------------------------------
bool stream_file(const std::string &filePath, Chunker &chunker) {
    std::ifstream ifs(filePath, std::ios::binary);
    if (!ifs.is_open()) {
        std::cerr << "Error: Could not open " << filePath << "\n";
        return false;
    }
    const size_t BUFSZ = 64 * 1024; // read in 64KB blocks
    char buffer[BUFSZ];

    while (!ifs.eof()) {
        ifs.read(buffer, BUFSZ);
        std::streamsize bytesRead = ifs.gcount();
        if (bytesRead > 0) {
            chunker.pushData(buffer, static_cast<size_t>(bytesRead));
        }
    }
    ifs.close();
    return true;
}

// -----------------------------------------------------------------------------
// 7) External Tools to convert PDF, DOC, ODT, RTF -> temp text file
//    Then we stream that temp text file into the chunker
// -----------------------------------------------------------------------------

// Utility to run a command with popen and read results into chunker
// Instead of storing in a string, we feed directly into the chunker in small blocks.
bool stream_command_output(const std::string &command, Chunker &chunker) {
    FILE *pipe = popen(command.c_str(), "r");
    if (!pipe) {
        std::cerr << "Error: popen failed for command: " << command << "\n";
        return false;
    }

    const size_t BUFSZ = 64 * 1024;
    char buffer[BUFSZ];
    while (!feof(pipe)) {
        size_t bytesRead = fread(buffer, 1, BUFSZ, pipe);
        if (bytesRead > 0) {
            chunker.pushData(buffer, bytesRead);
        }
    }
    int rc = pclose(pipe);
    if (rc != 0) {
        std::cerr << "Warning: command exited with code " << rc << ": " << command << "\n";
        // Not always an error—some tools exit non-zero for certain conditions
    }
    return true;
}

// For PDF:  "pdftotext <file> -"
bool stream_pdf(const std::string &filePath, Chunker &chunker) {
    std::string cmd = "pdftotext \"" + filePath + "\" -";
    return stream_command_output(cmd, chunker);
}

// DOC / DOCX: "doc2txt <file> -"
bool stream_doc(const std::string &filePath, Chunker &chunker) {
    // If doc2txt doesn't support "-", you may need a temp file approach
    std::string cmd = "doc2txt \"" + filePath + "\" -";
    return stream_command_output(cmd, chunker);
}

// ODT: "odt2txt --stdout <file>"
bool stream_odt(const std::string &filePath, Chunker &chunker) {
    std::string cmd = "odt2txt --stdout \"" + filePath + "\"";
    return stream_command_output(cmd, chunker);
}

// RTF: "unrtf --text <file>" -> parse from stdout
bool stream_rtf(const std::string &filePath, Chunker &chunker) {
    std::string cmd = "unrtf --text \"" + filePath + "\"";
    return stream_command_output(cmd, chunker);
}

// For CSV or TXT, we can read the file directly in binary mode
bool stream_csv_or_txt(const std::string &filePath, Chunker &chunker) {
    return stream_file(filePath, chunker);
}

// For unknown extension, attempt plain text read
bool stream_unknown(const std::string &filePath, Chunker &chunker) {
    return stream_file(filePath, chunker);
}

bool stream_parquet(const std::string &filePath, Chunker &chunker) {
    // 1. Open file as Arrow input
    auto infileResult = arrow::io::ReadableFile::Open(filePath);
    if (!infileResult.ok()) {
        std::cerr << "Error: Could not open Parquet file: " << filePath
        << "\n" << infileResult.status().ToString() << "\n";
        return false;
    }
    auto infile = *infileResult;

    // 2. Create a Parquet->Arrow reader
    std::unique_ptr<parquet::arrow::FileReader> reader;
    auto st_reader = parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader);
    if (!st_reader.ok()) {
        std::cerr << "Error: parquet::arrow::OpenFile failed: "
        << st_reader.ToString() << "\n";
        return false;
    }

    // 3. Iterate row-group by row-group
    int num_row_groups = reader->num_row_groups();
    for (int rg = 0; rg < num_row_groups; rg++) {
        std::shared_ptr<arrow::Table> table;
        auto st_rg = reader->ReadRowGroup(rg, &table);
        if (!st_rg.ok() || !table) {
            std::cerr << "Warning: Could not read row group " << rg
            << " from file " << filePath << "\n";
            continue;
        }
        int64_t num_rows = table->num_rows();
        int num_columns  = table->num_columns();

        // We'll build a textual line for each row. Then push to chunker.
        for (int64_t row_idx = 0; row_idx < num_rows; row_idx++) {
            std::ostringstream rowLine;
            for (int col_idx = 0; col_idx < num_columns; col_idx++) {
                // This is a ChunkedArray of data for column col_idx
                auto chunkedArr = table->column(col_idx);

                if (chunkedArr->num_chunks() < 1) {
                    rowLine << "[NO_CHUNKS]";
                } else {
                    auto arr = chunkedArr->chunk(0); // just first chunk example
                    if (row_idx < arr->length()) {
                        // Use GetScalar(row_idx)
                        auto scalar_result = arr->GetScalar(row_idx);
                        if (!scalar_result.ok()) {
                            // Can't retrieve the scalar for some reason
                            rowLine << "[ERROR: " << scalar_result.status().ToString() << "]";
                        } else {
                            auto scalar = *scalar_result;
                            if (scalar->is_valid) {
                                // Convert the scalar to string
                                rowLine << scalar->ToString();
                            } else {
                                rowLine << "NULL";
                            }
                        }
                    } else {
                        rowLine << "[out_of_range]";
                    }
                }

                if (col_idx < num_columns - 1) {
                    rowLine << " | ";
                }
            }
            rowLine << "\n";

            // push to chunker
            auto lineStr = rowLine.str();
            chunker.pushData(lineStr.data(), lineStr.size());
        }
    }

    return true;
}




// -----------------------------------------------------------------------------
// 8) Help message
// -----------------------------------------------------------------------------
void print_help(const char* progName) {
    std::cout << R"(

 ██████╗██╗  ██╗██╗   ██╗███╗   ██╗██╗  ██╗
██╔════╝██║  ██║██║   ██║████╗  ██║██║ ██╔╝
██║     ███████║██║   ██║██╔██╗ ██║█████╔╝
██║     ██╔══██║██║   ██║██║╚██╗██║██╔═██╗
╚██████╗██║  ██║╚██████╔╝██║ ╚████║██║  ██╗
 ╚═════╝╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝

)" << "\n";

std::cout << "\n--- Chunk Stream Program ---\n\n"
<< "Usage:\n"
<< "  " << progName << " <file1> [file2 ...]\n\n"
<< "Description:\n"
<< "  - Reads each file using external tools if needed (PDF, DOC, DOCX, ODT, RTF) or\n"
<< "    direct read (CSV, TXT).\n"
<< "  - Streams all data in ~5MB chunks, writes them as <sha512>_<timestamp>.txt\n"
<< "    in a directory under /tmp (like /tmp/chunked/chunked_<timestamp>).\n"
<< "  - Up to ~500MB total buffers are possible if multiple chunks are filling.\n\n"
<< "Requirements:\n"
<< "  - OpenSSL (libssl-dev) for SHA-512.\n"
<< "  - External commands: pdftotext, doc2txt, odt2txt, unrtf.\n\n"
<< "Example:\n"
<< "  " << progName << " big_file.pdf notes.csv doc1.docx\n\n";
}

// -----------------------------------------------------------------------------
// 9) Main
// -----------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_help(argv[0]);
        return 0;
    }

    // Create an output directory in /tmp
    std::time_t now = std::time(nullptr);
    std::ostringstream dirss;
    dirss << "/tmp/chunked_" << now;
    g_outputDir = dirss.str();

    if (mkdir(g_outputDir.c_str(), 0777) && errno != EEXIST) {
        std::cerr << "Error: Could not create directory: " << g_outputDir << "\n";
        return 1;
    }

    std::cout << "Chunks will be written to: " << g_outputDir << "\n";

    // Create a buffer pool (100 buffers => ~500MB if all in use)
    BufferPool bufferPool(NUM_BUFFERS, CHUNK_LIMIT);

    // Process each file
    for (int i = 1; i < argc; ++i) {
        std::string filePath = argv[i];
        std::cout << "\nProcessing file: " << filePath << "\n";

        // Derive extension
        std::string extension;
        {
            size_t pos = filePath.rfind('.');
            if (pos != std::string::npos) {
                extension = filePath.substr(pos + 1);
                std::transform(extension.begin(), extension.end(),
                               extension.begin(), ::tolower);
            }
        }

        // Acquire a chunker for this file
        Chunker chunker(bufferPool, g_outputDir);

        bool success = false;
        if (extension == "pdf") {
            success = stream_pdf(filePath, chunker);
        } else if (extension == "doc" || extension == "docx") {
            success = stream_doc(filePath, chunker);
        } else if (extension == "odt") {
            success = stream_odt(filePath, chunker);
        } else if (extension == "rtf") {
            success = stream_rtf(filePath, chunker);
        } else if (extension == "csv" || extension == "txt") {
            success = stream_file(filePath, chunker);
        } else if (extension == "parquet") {
            success = stream_parquet(filePath, chunker);
        } else {
            std::cout << "Unknown extension: " << extension
            << " -> attempting plain text read...\n";
            success = stream_file(filePath, chunker);
        }

        if (!success) {
            std::cerr << "Warning: No content processed from file: " << filePath << "\n";
        }
    }

    std::cout << "\nAll done. Chunks are located in: " << g_outputDir << "\n";
    return 0;
}
