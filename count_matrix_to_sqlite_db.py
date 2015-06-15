#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Create a SQLite database from a ASCII count matrix."""

from __future__ import print_function
import argparse
import sqlite3 
from itertools import islice, chain
import sys

__author__ = "Florian Plaza Oñate"
__copyright__ = "Copyright 2015, Enterome"
__version__ = "1.0.0"
__maintainer__ = "Florian Plaza Oñate"
__email__ = "fplaza-onate@enterome.com"
__status__ = "Development"

def get_parameters():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('--input-file', '-i', dest='count_matrix_txt', required=True, 
            help='Count matrix with an header listing all the samples names. (tab-separated values file)')

    parser.add_argument('--output-file', '-o', dest='count_matrix_db', required=True,
            help='Generated SQLite database containing the count matrix data.')

    return parser.parse_args()

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

def read_samples_names(count_matrix_txt):
    with open(count_matrix_txt) as istream:
        header = istream.next()
        samples_names = header.split()

        if not header.startswith('\t'):
            samples_names = header_items[1:]

    print('Input file has {} samples'.format(len(samples_names)))

    return samples_names

def read_genes_profiles(istream):
    istream.next()

    for line in istream:
        line_items = line.split()
        gene_id = line_items[0]
        gene_profile = map(float, line_items[1:])
        yield([gene_id] + gene_profile)

def create_count_matrix_table(count_matrix_db, samples_names):
    req = 'CREATE TABLE COUNT_MATRIX(GENE_ID PRIMARY KEY NOT NULL,'
    req += ','.join(( '"' + sample_name + '" REAL' for sample_name in samples_names)) + ');'

    conn = sqlite3.connect(count_matrix_db)
    c = conn.cursor()
    c.execute(req)
    conn.close()

def fill_count_matrix_table(count_matrix_db, count_matrix_txt, num_samples, batch_size=1000):
    req = 'INSERT INTO COUNT_MATRIX VALUES ('
    req += ','.join('?'*(num_samples+1))
    req += ');'

    conn = sqlite3.connect(count_matrix_db)
    c = conn.cursor()

    with open(count_matrix_txt) as istream:
        for i, genes_profiles in enumerate(batch(read_genes_profiles(istream),batch_size),start=1):
            c.executemany(req, genes_profiles)
            print("{} records inserted".format(i*batch_size))
            sys.stdout.write("\033[F")


    conn.close()

def main():
    parameters = get_parameters()
    samples_names = read_samples_names(parameters.count_matrix_txt)
    create_count_matrix_table(parameters.count_matrix_db, samples_names)
    fill_count_matrix_table(parameters.count_matrix_db,  parameters.count_matrix_txt, len(samples_names))

if __name__ == '__main__':
    main()
