import random
import string

alphabet = (string.ascii_lowercase + ' ').encode('ascii')


def random_string(length: int):
    return bytes(random.choices(alphabet, k=length)).decode('ascii')


def generate_file(path: str, num_lines: int, string_len: int):
    with open(path, mode='w') as fout:
        for i in range(num_lines):
            fout.write(random_string(string_len))
            fout.write('\n')
