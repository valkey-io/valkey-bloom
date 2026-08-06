import struct
import random
import socket
import os

def power_law_random(alpha=2.0):
    # Simple power law distribution approximation
    x = random.random()
    return int((1.0 - x) ** (-1.0 / (alpha - 1.0))) + 1

def generate_ipv4_data(filename_prefix, file_num, num_entries, alpha, reservoir_size):
    filename = f"{filename_prefix}{file_num}.dat"
    reservoir = []
    for i in range(num_entries):
        # Generate source and destination IPs
        srcIP = socket.inet_aton(".".join(map(str, (random.randint(0, 255) for _ in range(4)))))
        dstIP = socket.inet_aton(".".join(map(str, (random.randint(0, 255) for _ in range(4)))))
        
        # Generate ports
        srcPort = struct.pack('>H', random.randint(0, 65535))
        dstPort = struct.pack('>H', random.randint(0, 65535))
        
        # Generate protocol (TCP=6 or UDP=17)
        protocol = struct.pack('>B', random.choice([6, 17]))
        
        # Assemble in new order: src_ip, src_port, dst_ip, dst_port, protocol
        tuple_data = srcIP + srcPort + dstIP + dstPort + protocol
        
        count = power_law_random(alpha)
        for _ in range(count):
            if i < reservoir_size:
                reservoir.append(tuple_data)
            elif i >= reservoir_size and random.random() < reservoir_size / float(i+1):
                reservoir[random.randint(0, reservoir_size-1)] = tuple_data
    
    with open(filename, 'wb') as f:
        for entry in reservoir:
            f.write(entry)

# Generate IPv4 data
for i in range(11):  # Generate 0.dat through 10.dat
    generate_ipv4_data('', i, 1000, 2.0, 1000)
