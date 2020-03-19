import argparse
import json
import os
import time
from multiprocessing.pool import Pool

import tqdm


class Iterator:
    def __init__(self, i):
        self.i = i

    def __len__(self):
        with open(self.i, "r") as f:
            for i, l in enumerate(f):
                pass
        return i + 1

    def __iter__(self):
        self.f = open(self.i, "r")
        return self

    def __next__(self):
        line = self.f.readline()
        if line == "":
            raise StopIteration
        line = line.replace("\n", "")
        return line


def func(param):
    return None


def run_pool(iterator: Iterator, function, out_file: str, dump_time_step: int, threads: int) -> None:
    total_items = len(iterator)

    # Load if exists
    if os.path.exists(out_file):
        with open(out_file, "r") as p:
            computed = json.load(p)
    else:
        computed = {}

    # Begin
    t1 = time.time()
    p_list = []
    progress_bar = tqdm.tqdm(total=total_items)
    progress_bar.update(len(computed))
    for i, param in enumerate(iterator):
        if param in computed:
            continue
        p_list.append(param)
        if len(p_list) == threads or i == total_items - 1:
            with Pool(threads) as pool:
                # Launch pool of threads computing the function
                all_results = pool.map(function, p_list)
                for r, p in zip(all_results, p_list):
                    computed[p] = r
            progress_bar.update(len(p_list))
            p_list = []
        t2 = time.time()

        # Dump each dump_time_step seconds
        if t2 - t1 > dump_time_step:
            with open(out_file, "w") as p:
                json.dump(computed, open(out_file, "w"))
            t1 = time.time()
    print("Done")


def get_parser():
    parser = argparse.ArgumentParser(description='Compute function in a pool of threads')
    parser.add_argument('-i', type=str, default="input.txt", help='input argument')
    parser.add_argument('-o', type=str, default="output.txt", help='output file')
    parser.add_argument('--dump_time_step', type=int, default=60, help='secs to dump')
    parser.add_argument('--threads', type=int, default=4, help='secs to dump')

    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()
    iterator = Iterator(args.i)
    run_pool(iterator, func, args.o, args.dump_time_step, args.threads)
