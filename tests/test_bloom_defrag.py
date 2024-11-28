import time
from valkeytests.valkey_test_case import ValkeyAction
from valkey_bloom_test_case import ValkeyBloomTestCaseBase
from valkeytests.conftest import resource_port_tracker

class TestBloomDefrag(ValkeyBloomTestCaseBase):
    
    def get_custom_args(self):
        args = super().get_custom_args()
        # args.update({'activedefrag': 'yes'})

        args.update({'activedefrag': 'yes', 'active-defrag-threshold-lower': '0', 'active-defrag-ignore-bytes': '1'})
        return args
    
    def test_bloom_defrag(self):
        stats = self.parse_valkey_stats()
        defrag_hits = int(stats.get('active_defrag_hits', 0))
        defrag_misses = int(stats.get('active_defrag_misses', 0))
        assert defrag_hits == 0
        assert defrag_misses == 0
        mem_info = self.client.execute_command('INFO MEMORY ')
        print(mem_info)
        print("\n\n\n\n")

        self.client.execute_command(command)

        # setting max_memory through config set

        # defrag_misses = self.client.execute_command('INFO STATS')["active_defrag_misses"]
        # assert defrag_misses == 0
        scale_names = [f'scale_{i}' for i in range(1, 1000)]

        # Loop through the scale names and execute the command
        for scale in scale_names:
            command = f'bf.insert {scale} CAPACITY 1 EXPANSION 1 ITEMS 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17'
            self.client.execute_command(command)

        time.sleep(1)
        mem_info = self.client.execute_command('INFO MEMORY ')
        stats = self.parse_valkey_stats()
        defrag_hits = int(stats.get('active_defrag_hits', 0))
        defrag_misses = int(stats.get('active_defrag_misses', 0))
        print(f"Active defrag hits: {defrag_hits}")
        print(f"Active defrag misses: {defrag_misses}")
        print(mem_info)

        # unlink_count = int(len(scale_names) * 0.8)

        # for scale in scale_names[:unlink_count]:
        #     self.client.execute_command(f'DEL {scale}')


        # mem_info = self.client.execute_command('INFO MEMORY ')
        # print("\n\n\n\n")

        # print(mem_info)
        assert defrag_hits == 0
        assert defrag_misses == 0


    def parse_valkey_stats(self):
        mem_info = self.client.execute_command('INFO STATS \n\n\n')

        # Split the string into lines
        lines = mem_info.decode('utf-8').split('\r\n')
        
        # Create a dictionary to store the key-value pairs
        stats_dict = {}
        
        # Parse each line
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                stats_dict[key.strip()] = value.strip()
        
        return stats_dict