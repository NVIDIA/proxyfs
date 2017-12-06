Data Verifier is a simple test suite to write a known data and then read back to verify if the data is correct.

To run data verifier for create a reference file - make sure it is not on proxyfs FS so that we have a trusted reference file.
Example - create a 1GB file in the local test directory:

dd if=/dev/urandom of=reference_file bs=1M count=1024

Build the binary to run:
Balajis-MacBook-Pro-2:data_verifier balajirao$ go build

Balajis-MacBook-Pro-2:data_verifier balajirao$ ./data_verifier --help
Usage of ./data_verifier:
  -flushSize int
        Outstanding data before explicit flush (default 1048576)
  -ioSize int
        IO Size for reading and writing (default 65536)
  -numThreads int
        Number of writer threads (default 1)
  -refFile string
        Reference file for reading test data
  -sync
        Sync After Every write
  -tgtDir string
        Target Directory for writing test files


Running data verifier:
1] with defaults:
./data_verifier -tgtDir /Volumes/proxyfs/verify/ -refFile ./ref_file

2] with 10 concurrent threads:
./data_verifier -tgtDir /Volumes/proxyfs/verify/ -refFile ./ref_file -numThreads 10

3] with sync after every write:
./data_verifier -tgtDir /Volumes/proxyfs/verify/ -refFile ./ref_file -numThreads 10 -sync