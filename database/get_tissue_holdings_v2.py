#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
(c) 04/13/2019 Brant Faircloth || http://faircloth-lab.org/
All rights reserved.

This code is distributed under a 3-clause BSD license. Please see
LICENSE.txt for more information.

Created on Jun 13, 2018 at 09:50:25.
"""

import os
import sys
import copy
import math
import argparse
import configparser
from datetime import date
from itertools import cycle

import pandas as pd
import numpy
from sqlalchemy import create_engine

import pdb


class FullPaths(argparse.Action):
    """Expand user- and relative-paths"""
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, os.path.abspath(os.path.expanduser(values)))


def is_file(filename):
    """check if something is a file"""
    if not os.path.isfile:
        msg = "{0} is not a file".format(filename)
        raise argparse.ArgumentTypeError(msg)
    else:
        return filename


def get_args():
    parser = argparse.ArgumentParser(
        description="""Given a config file of species, return db records""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--species-spreadsheet',
        required=True,
        type=is_file,
        action=FullPaths,
        help="""The file containing the species names to query."""
    )
    parser.add_argument(
        '--species-excludes',
        required=False,
        type=is_file,
        action=FullPaths,
        help="""A file containing any species to exclude."""
    )
    parser.add_argument(
        '--all-databases',
        action='store_true',
        default=False,
        help="""Run queries against all possible database."""
    )
    return parser.parse_args()



def check_alternate_taxonomies(connection, taxon):
    tax = []
    df = pd.read_sql_query("""
        SELECT
            alt_genus, alt_species
        FROM
            taxonomies
        WHERE
            genus='{0}' AND species='{1}'
        """.format(taxon[0], taxon[1]), con=connection)
    # drop the duplicates
    unique = df.drop_duplicates()
    # iterrate over each row
    for index, row in unique.iterrows():
        alt_taxon_names = (row.alt_genus, row.alt_species)
        tax.append(alt_taxon_names)
    return tax

def check_ioc_taxonomies(connection, taxon):
    tax = []
    df = pd.read_sql_query("""
        SELECT
            genus, species
        FROM
            taxonomies
        WHERE
            alt_genus='{0}' AND alt_species='{1}'
        """.format(taxon[0], taxon[1]), con=connection)
    if len(df) > 0:
        # be sure to include non-ioc taxonomy
        ioc_taxon_names = (taxon[0], taxon[1])
        tax.append(ioc_taxon_names)
        # drop the duplicates
        unique = df.drop_duplicates()
        # iterrate over each row
        for index, row in unique.iterrows():
            ioc_taxon_names = (row.genus, row.species)
            tax.append(ioc_taxon_names)
    else:
        ioc_taxon_names = (taxon[0], taxon[1])
        tax.append(ioc_taxon_names)
    return tax


def check_species_list_against_ref_taxonomy(connection, sheet):
    ioc_taxa = []
    non_ioc_taxa = []
    for index, row in sheet.iterrows():
        #pdb.set_trace()
        if (row['Status'] is numpy.nan) or math.isnan(row['Status']):
            genus, species = row['Genus species'].split(' ')
            query = connection.execute("""
                SELECT
                    genus, species
                FROM
                    species
                WHERE
                    genus='{0}' AND SPECIES='{1}'
                """.format(genus, species))
            try:
                assert query.rowcount==1
                taxon_names = []
                for row in query:
                    ioc_taxa.append(row)
            except AssertionError:
                non_ioc_taxa.append((genus, species))
        elif row['Status'].lower() == "completed":
            pass
    return ioc_taxa, non_ioc_taxa

def get_tissue_records(connection, taxon):
    if len(taxon) == 1:
        other_tissue_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                tissues
            WHERE 
                (genus='{0}' AND species='{1}')
            ORDER BY 
                sex, year DESC 
            LIMIT 25
            """.format(taxon[0][0], taxon[0][1]), con=connection)
    elif len(taxon) > 1:
        # format the where statement
        partial_where = ["(genus='{0}' AND species='{1}')".format(name[0], name[1]) for name in taxon]
        where = " OR ".join(partial_where)
        other_tissue_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                tissues 
            WHERE 
                ({0})
            ORDER BY 
                sex ASC, year DESC 
            LIMIT 25
            """.format(where), con=connection)
    return other_tissue_df


def get_vertnet_tissue_records(connection, taxon):
    if len(taxon) == 1:
        vertnet_tissue_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                v_vertnet
            WHERE 
                (genus='{0}' AND species='{1}') 
            ORDER BY 
                sex, year DESC 
            LIMIT 25
            """.format(taxon[0][0], taxon[0][1]), con=connection)
    elif len(taxon) > 1:
        # format the where statement
        partial_where = ["(genus='{0}' AND species='{1}')".format(name[0], name[1]) for name in taxon]
        where = " OR ".join(partial_where)
        vertnet_tissue_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                v_vertnet 
            WHERE 
                ({0})
            ORDER BY 
                sex ASC, year DESC
            LIMIT 25
            """.format(where), con=connection)
    return vertnet_tissue_df


def get_ala_tissue_records(connection, taxon):
    if len(taxon) == 1:
        ala_tissue_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                v_ala
            WHERE 
                (genus='{0}' AND species='{1}') 
            ORDER BY 
                sex, year DESC 
            LIMIT 25
            """.format(taxon[0][0], taxon[0][1]), con=connection)
    elif len(taxon) > 1:
        # format the where statement
        partial_where = ["(genus='{0}' AND species='{1}')".format(name[0], name[1]) for name in taxon]
        where = " OR ".join(partial_where)
        ala_tissue_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                v_ala 
            WHERE 
                ({0})
            ORDER BY 
                sex ASC, year DESC
            LIMIT 25
            """.format(where), con=connection)
    return ala_tissue_df


if __name__ == '__main__':
    print("Starting.\n")
    args = get_args()
    db_conf = configparser.ConfigParser()
    db_conf.read("access.conf")
    connection_string = "postgresql://{0}:{1}@localhost:5432/openwings".format(
            db_conf['openwings']['user'],
            db_conf['openwings']['password']
        )
    engine = create_engine(connection_string)
    con = engine.connect()
    # read in spreadsheet with values for genus and species
    sheet = pd.read_excel(args.species_spreadsheet)
    # get taxa to exclude
    excludes = pd.read_excel(args.species_excludes)
    excludes_set = set(excludes['Type Genera'])
    print("Checking Taxonomy...\n")
    # get dictionaries of starting taxon names + any other taxonomies (for IOC taxa)
    tmp_ioc_taxa, tmp_non_ioc_taxonomy = check_species_list_against_ref_taxonomy(con, sheet)
    # if they are, get all the alt_genus/alt_species names we can
    ioc_taxonomy = {}
    non_ioc_taxonomy = {}
    print("Getting taxonomies for IOC taxa")
    cycled_color = cycle(range(2))
    # create master dataframe for tissues
    #master_lsu_tissues = pd.DataFrame()
    master_other_tissues = pd.DataFrame()
    master_vertnet_tissues = pd.DataFrame()
    master_ala_tissues = pd.DataFrame()
    master_missing_tissues = pd.DataFrame()
    for taxon in tmp_ioc_taxa:
        tax = check_alternate_taxonomies(con, taxon)
        ioc_taxonomy[' '.join(taxon)] = tax
    for taxon in tmp_non_ioc_taxonomy:
        tax = check_ioc_taxonomies(con, taxon)
        non_ioc_taxonomy[' '.join(taxon)] = tax
    # combine both the dictionaries
    all_taxonomies = {**ioc_taxonomy, **non_ioc_taxonomy}
    for taxon, taxa_names in all_taxonomies.items():
        print(taxon)
        other_tissue_records = get_tissue_records(con, taxa_names)
        # get vertnet tissue records
        vertnet_tissue_records = get_vertnet_tissue_records(con, taxa_names)
        # get ALA tissue records
        ala_tissue_records = get_ala_tissue_records(con, taxa_names)
        # extract the starting information from the spreadsheet
        sheet_info = sheet.loc[sheet['Genus species'] == taxon]
        # if we cant find any taxon in any database, that taxon is missing
        if args.all_databases:
            if (len(other_tissue_records) == 0) and (len(vertnet_tissue_records) == 0) and (len(ala_tissue_records) == 0):
                master_missing_tissues = master_missing_tissues.append(sheet_info,ignore_index=True)
        else:
            if len(other_tissue_records) == 0:
                master_missing_tissues = master_missing_tissues.append(sheet_info,ignore_index=True)
        ########################
        # OTHER COLLAB TISSUES #
        ########################
        #pdb.set_trace()
        if taxon in excludes_set:
            print("\tskipped")
            # create fake, empty data frame - we want to exclude data here
            other_tissue_records = pd.DataFrame(columns=['icode','year','catalognumber','sex','ordr','family','genus','species','subspecies','preparations','continent','country','state','county','island','locality','decimallatitude','decimallongitude','remarks','prep','rank'])
            # set status == skipped
            other_sheet_info = copy.deepcopy(sheet_info)
            skipped = True
        elif len(other_tissue_records) >= 1:
            other_sheet_info = pd.concat([sheet_info] * len(other_tissue_records))
            skipped = False
        else:
            other_sheet_info = copy.deepcopy(sheet_info)
            skipped = False
        #pdb.set_trace()
        # reset index on sheet_info
        other_sheet_info = other_sheet_info.reset_index()
        # join these two df
        other_tissue_df = pd.concat([other_sheet_info, other_tissue_records], axis=1, sort=False)
        # add a column indicating which color we'll use for records
        color = next(cycled_color)
        if not skipped:
            other_tissue_df['Color'] = color
        else:
            other_tissue_df['Color'] = 3
        #pdb.set_trace()
        if len(other_tissue_records) >= 1:
            other_tissue_df['missing'] = False
        elif skipped:
            other_tissue_df['missing'] = 'GENUS'
        else:
            other_tissue_df['missing'] = True
        #pdb.set_trace()
        master_other_tissues = master_other_tissues.append(other_tissue_df,ignore_index=True)
        if args.all_databases:
            ###################
            # VERTNET TISSUES #
            ###################
            if len(vertnet_tissue_records) > 1:
                vertnet_sheet_info = pd.concat([sheet_info] * len(vertnet_tissue_records))
            else:
                vertnet_sheet_info = copy.deepcopy(sheet_info)
            # reset index on sheet_info
            vertnet_sheet_info = vertnet_sheet_info.reset_index()
            # join these two df
            vertnet_tissue_df = pd.concat([vertnet_sheet_info, vertnet_tissue_records], axis=1, sort=False)
            # add a column indicating which color we'll use for records
            vertnet_tissue_df['Color'] = color
            if len(vertnet_tissue_records) == 0:
                vertnet_tissue_df['Missing'] = True
            else:
                vertnet_tissue_df['Missing'] = False
            master_vertnet_tissues = master_vertnet_tissues.append(vertnet_tissue_df,ignore_index=True)
            ###############
            # ALA TISSUES #
            ###############
            if len(ala_tissue_records) > 1:
                ala_sheet_info = pd.concat([sheet_info] * len(ala_tissue_records))
            else:
                ala_sheet_info = copy.deepcopy(sheet_info)
            # reset index on sheet_info
            ala_sheet_info = ala_sheet_info.reset_index()
            # join these two df
            ala_tissue_df = pd.concat([ala_sheet_info, ala_tissue_records], axis=1, sort=False)
            # add a column indicating which color we'll use for records
            ala_tissue_df['Color'] = color
            if len(ala_tissue_records) == 0:
                ala_tissue_df['Missing'] = True
            else:
                ala_tissue_df['Missing'] = False
            master_ala_tissues = master_ala_tissues.append(ala_tissue_df,ignore_index=True)

    #master_lsu_tissues.to_excel('{}-lsu_merged_tissues.xlsx'.format(date.today()))
    master_other_tissues.to_excel('{}-other_merged_tissues.xlsx'.format(date.today()))
    master_missing_tissues.to_excel('{}-missing_tissues.xlsx'.format(date.today()))
    if args.all_databases:
        master_vertnet_tissues.to_excel('{}-vertnet_merged_tissues.xlsx'.format(date.today()))
        master_ala_tissues.to_excel('{}-ala_merged_tissues.xlsx'.format(date.today()))
    con.close()
    print("\nFinished.\n")