#!/usr/bin/python
# -*- coding: utf-8 -*-

"""Create a SQLite database from a ASCII count matrix."""

from __future__ import print_function
import argparse
import sqlite3 
from itertools import islice, izip, chain
import sys
import os

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

    parser.add_argument('--samples-chunk-size', dest='samples_chunk_size', type=int, default=500,
            help='')

    parser.add_argument('--output-db', '-o', dest='output_db', required=True,
            help='Generated SQLite database.')


    return parser.parse_args()


def get_samples(count_matrix_txt, samples_chunk_size):
    with open(count_matrix_txt) as istream:
        header = istream.next()
        header_items = header.rstrip().split('\t')
        samples = header_items[1:]

    samples_chunks = [(i,min(i + samples_chunk_size, len(samples))) for i in range(0, len(samples), samples_chunk_size)]

    return samples, samples_chunks

def create_tables(samples, samples_chunks, output_db):
    genes_tbl_req = (
            'CREATE TABLE genes'
            '(gene_id INTEGER PRIMARY KEY AUTOINCREMENT, gene_name TEXT NOT NULL);')

    genes_profiles_tbl_reqs=list()
    for chunk_id in range(0,len(samples_chunks)):
        samples_chunk_beg,samples_chunk_end = samples_chunks[chunk_id]
        genes_profiles_tbl_name='genes_profiles_{}'.format(chunk_id+1)
        genes_profiles_tbl_req = 'CREATE TABLE {}'.format(genes_profiles_tbl_name)
        genes_profiles_tbl_req += '(gene_id INTEGER PRIMARY KEY AUTOINCREMENT,' 
        genes_profiles_tbl_req += ','.join(( '"' + sample_name + '" REAL' for sample_name in samples[samples_chunk_beg:samples_chunk_end])) + ');'
        genes_profiles_tbl_reqs.append(genes_profiles_tbl_req)

    with sqlite3.connect(output_db) as conn:
        conn.execute(genes_tbl_req)

        for genes_profiles_tbl_req in genes_profiles_tbl_reqs:
            conn.execute(genes_profiles_tbl_req)


def read_genes_profiles(istream):
    istream.next()

    for line in istream:
        line_items = line.split()
        gene_name = line_items[0]
        gene_profile = map(float, line_items[1:])
        yield ([gene_name] + gene_profile)

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        yield chain([batchiter.next()], batchiter)

def fill_tables(count_matrix_txt, samples_chunks, output_db, batch_size=1000):
    genes_tbl_req = (
            'INSERT INTO genes '
            'VALUES(NULL,?)')

    genes_profiles_tbl_reqs = list()
    for i in range(0,len(samples_chunks)):
        samples_chunk_size = samples_chunks[i][1]-samples_chunks[i][0]
        genes_profiles_tbl_name='genes_profiles_{}'.format(i+1)
        genes_profiles_tbl_req = 'INSERT INTO {} VALUES(NULL,'.format(genes_profiles_tbl_name)
        genes_profiles_tbl_req += ','.join('?'*samples_chunk_size) + ');'
        genes_profiles_tbl_reqs.append(genes_profiles_tbl_req)

    with open(count_matrix_txt) as istream, sqlite3.connect(output_db) as conn:
        num_genes_processed=0
        for genes_profiles in batch(read_genes_profiles(istream),batch_size):
            genes_names, genes_profiles = izip(*(((item[0],),item[1:]) for item in genes_profiles))
            conn.executemany(genes_tbl_req, genes_names)

            for chunk_id, genes_profiles_tbl_req in enumerate(genes_profiles_tbl_reqs):
                samples_chunk_beg,samples_chunk_end = samples_chunks[chunk_id]
                genes_profiles_chunk=(gene_profile[samples_chunk_beg:samples_chunk_end] for gene_profile in genes_profiles)
                conn.executemany(genes_profiles_tbl_req, genes_profiles_chunk)

            num_genes_processed+= len(genes_profiles)

            sys.stdout.write("\033[F")
            print("{} genes profiles processed".format(num_genes_processed))

def create_genes_names_index(output_db):
    req =('CREATE UNIQUE INDEX genes_names_index '
            'ON genes(gene_name);')

    with sqlite3.connect(output_db) as conn:
        conn.execute(req)

def main():
    parameters = get_parameters()
    if os.path.isfile(parameters.output_db):
        raise sqlite3.OperationalError('{} exists'.format(parameters.output_db))

    samples, samples_chunks = get_samples(parameters.count_matrix_txt, parameters.samples_chunk_size)
    print('Count matrix file has {} samples\n'.format(len(samples)))

    create_tables(samples, samples_chunks, parameters.output_db)
    fill_tables(parameters.count_matrix_txt, samples_chunks, parameters.output_db)

    #if (parameters.index_genes_names):
        #print("Creating a unique index on genes names")
        #create_genes_names_index(parameters.output_db)

if __name__ == '__main__':
    main()
