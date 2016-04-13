#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Create a SQLite database from a ASCII count matrix."""

from __future__ import print_function
import argparse
import sqlite3 
from itertools import islice, izip, chain
import sys

__author__ = "Florian Plaza Oñate"
__copyright__ = "Copyright 2015, Enterome"
__version__ = "1.0.0"
__maintainer__ = "Florian Plaza Oñate"
__email__ = "fplaza-onate@enterome.com"
__status__ = "Development"

def get_parameters():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('--count-matrix', '-i', dest='count_matrix_txt', required=True, 
            help='Count matrix with an header listing all the samples names. (tab-separated values file)')

    parser.add_argument('--index-genes-names', dest='index_genes_names', action='store_true', default=False,
            help='Indicates whether an index on genes names should be created.')

    parser.add_argument('--output-db', '-o', dest='output_db', required=True,
            help='Generated SQLite database.')


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
            samples_names = samples_names[1:]

    print('Count matrix file has {} samples\n'.format(len(samples_names)))

    return samples_names

def read_genes_profiles(istream):
    istream.next()

    for line in istream:
        line_items = line.split()
        gene_name = line_items[0]
        gene_profile = map(float, line_items[1:])
        yield ([gene_name] + gene_profile)

def create_tables(samples_names, output_db):
    genes_tbl_req = (
            'CREATE TABLE genes'
            '(gene_id INTEGER PRIMARY KEY AUTOINCREMENT, gene_name TEXT NOT NULL);')

    genes_profiles_tbl_req = (
            'CREATE TABLE genes_profiles'
            '(gene_id INTEGER PRIMARY KEY AUTOINCREMENT,' )
    genes_profiles_tbl_req += ','.join(( '"' + sample_name + '" REAL' for sample_name in samples_names)) + ');'

    with sqlite3.connect(output_db) as conn:
        conn.execute(genes_tbl_req)
        conn.execute(genes_profiles_tbl_req)

def fill_tables(count_matrix_txt, num_samples,output_db, batch_size=1000):
    genes_tbl_req = (
            'INSERT INTO genes '
            'VALUES(NULL,?)')

    genes_profiles_tbl_req = 'INSERT INTO genes_profiles VALUES(NULL,'
    genes_profiles_tbl_req += ','.join('?'*num_samples) + ');'

    with open(count_matrix_txt) as istream, sqlite3.connect(output_db) as conn:
        for i, genes_profiles in enumerate(batch(read_genes_profiles(istream),batch_size),start=1):
            genes_names, genes_profiles = izip(*(((item[0],),item[1:]) for item in genes_profiles))
            conn.executemany(genes_tbl_req, genes_names)
            conn.executemany(genes_profiles_tbl_req, genes_profiles)

            sys.stdout.write("\033[F")
            print("{} genes profiles processed".format(i*batch_size))

def create_genes_names_index(output_db):
    req =(
            'CREATE UNIQUE INDEX genes_names_index'
            'ON genes(gene_name);')

    with sqlite3.connect(output_db) as conn:
        conn.execute(req)

def main():
    parameters = get_parameters()
    samples_names = read_samples_names(parameters.count_matrix_txt)
    create_tables(samples_names, parameters.output_db)
    fill_tables(parameters.count_matrix_txt, len(samples_names), parameters.output_db)
    if (parameters.index_genes_names):
        print("Creating a unique index on genes names")
        create_genes_names_index(parameters.output_db)

if __name__ == '__main__':
    main()
