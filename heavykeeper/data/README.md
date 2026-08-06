# Test Data

## Downloading Test Data

To download the test data files, you'll need Python with the `requests` and `tqdm` packages installed. Run:

```bash
pip install requests tqdm
python download_testdata.py
```

This script will download CAIDA trace files (0.dat through 10.dat) that are used for testing and benchmarking the Jigsaw Sketch implementation. The files will be downloaded to the current directory.

## Data Description

The downloaded files contain network flow data in binary format from the CAIDA dataset. Each file contains a sequence of network flow records, which are used to test and benchmark the Jigsaw Sketch's ability to identify elephant flows (high-volume network flows) in high-speed networks.

The data files (0.dat through 10.dat) are sourced from the CAIDA dataset and contain anonymized network flow records. Each record represents a network flow with its associated metadata, making them ideal for testing the Jigsaw Sketch's performance in identifying high-volume flows in network traffic analysis.

## Data Format
- Each file (0.dat through 10.dat) contains IPv4 5-tuple flow records
- Each record is 13 bytes with fields in this order:
  - Source IP (4 bytes)
  - Source Port (2 bytes)
  - Destination IP (4 bytes)
  - Destination Port (2 bytes)
  - Protocol (1 byte)

Example record:
```hex
99 c1 8a e6 01 bb db 0a ab 60 9a 9a 06
[  src_ip  ] [sp] [  dst_ip  ] [dp] [p]
```

## Data Source
These files are from the CAIDA dataset, provided by [DHS](https://github.com/ZeBraHack0/DHS/tree/main/data) for testing sketch performance. For other uses of CAIDA datasets, please register with CAIDA and apply for access.

## Verification
After downloading, you should have:
- 0.dat through 10.dat
- Each file contains binary 5-tuple IPv4 flow records
- Files are used by the flow_processor example to test sketch performance

### File Format Verification
You can verify the file format using hexdump:
```bash
hexdump -C 0.dat | head -n 1
```


## Use Synthetic Data
Alternatively, you can generate synthetic data with the script 
```bash
python generate.py
```
