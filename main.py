import argparse
import sys
import multiprocessing

from external_sorting.generator import generate_file
from external_sorting.sort import SortRunner, log
from external_sorting.constants import BUF_SIZE, SORT_MEMORY


def main(args):
    try:
        if 'gen' in args.mode:
            log.info(f'Generating dummy file {args.input} with {args.num_lines} lines (string_len={args.string_len})')
            generate_file(args.input, args.num_lines, args.string_len)
            log.info(f'File with {args.num_lines} (string_len={args.string_len}) lines was generated')

        if 'sort' in args.mode:
            SortRunner(args.input, args.output, args.num_workers, args.bs, args.sort_memory).run()

    except Exception as ex:
        log.exception(ex)
        sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='External Sorting Algorithm')
    parser.add_argument('--mode', choices=['gen', 'sort', 'gensort'], default='gensort')
    parser.add_argument('--input', default='./file.txt', help='Input txt file')
    parser.add_argument('--output', default='./file_sorted.txt', help='Output txt file')
    parser.add_argument('--num_workers', default=multiprocessing.cpu_count(), type=int)
    parser.add_argument('--string_len', default=1000, type=int, help='Length of each string during generation')
    parser.add_argument('--num_lines', default=1000000, type=int, help='Number of lines to be generated')
    parser.add_argument('--sort_memory', default=SORT_MEMORY, type=int, help='Memory limit during sorting step')
    parser.add_argument('--bs', default=BUF_SIZE, type=int, help='Buffer size during merge step')
    main(parser.parse_args())
