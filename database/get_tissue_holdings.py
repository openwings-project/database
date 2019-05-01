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
        '--species-config',
        required=True,
        type=is_file,
        action=FullPaths,
        help="""The file containing the species names to query."""
    )
    return parser.parse_args()



def get_holdings_species(connection, museum):
    df = pd.read_sql_query('''
        SELECT
            custom_species_temp.genus,
            custom_species_temp.species
        FROM
            {0},
            custom_species_temp
        WHERE
            custom_species_temp.genus={0}.genus
            AND custom_species_temp.species={0}.species
        GROUP BY
            custom_species_temp.genus, custom_species_temp.species;
        '''.format(museum), con=connection)
    return df


def get_holdings_species_by_taxonomy(connection, museum, taxonomy):
    df = pd.read_sql_query("""
        SELECT
            custom_species_temp.genus,
            custom_species_temp.species
        FROM
            {0},
            custom_species_temp,
            taxonomies
        WHERE
            taxonomies.taxonomy_id={1}
            AND custom_species_temp.genus = taxonomies.genus
            AND custom_species_temp.species = taxonomies.species
            AND (custom_species_temp.genus != taxonomies.alt_genus OR custom_species_temp.species != taxonomies.alt_species)
            AND {0}.genus = taxonomies.alt_genus
            AND {0}.species = taxonomies.alt_species
        GROUP BY
            custom_species_temp.genus, custom_species_temp.species;
        """.format(museum, taxonomy), con=connection)
    return df


def get_temp_table_of_reference_taxonomy_species(connection, taxa):
    text = " OR ".join(["(genus='{}' AND species='{}')".format(taxon[0], taxon[1]) for taxon in taxa])
    connection.execute("""
        CREATE TEMPORARY TABLE custom_species_temp AS
        SELECT
            species.genus, species.species
        FROM
            species
        WHERE
            {0};
        """.format(text), con=connection)
    df = pd.read_sql_query("""
        SELECT
            genus, species
        FROM
            custom_species_temp;
        """.format(text), con=connection)
    return df

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
        if row['Status'] is numpy.nan:
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

def get_lsu_tissue_records(connection, taxon):
    if len(taxon) == 1:
        lsu_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                v_lsumns_update 
            WHERE 
                genus='{0}' AND species='{1}' AND b_num IS NOT NULL 
            ORDER BY 
                sex, year DESC 
            LIMIT 15
            """.format(taxon[0][0], taxon[0][1]), con=connection)
    elif len(taxon) > 1:
        # format the where statement
        partial_where = ["(genus='{0}' AND species='{1}')".format(name[0], name[1]) for name in taxon]
        where = " OR ".join(partial_where)
        lsu_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                v_lsumns_update 
            WHERE 
                ({0}) AND b_num IS NOT NULL
            ORDER BY 
                sex ASC, year DESC 
            LIMIT 15
            """.format(where), con=connection)
    return lsu_df

def get_other_tissue_records(connection, taxon):
    if len(taxon) == 1:
        other_tissue_df = pd.read_sql_query("""
            SELECT
                *
            FROM
                tissues
            WHERE 
                (genus='{0}' AND species='{1}') AND icode!='LSUMNS'
            ORDER BY 
                sex, year DESC 
            LIMIT 15
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
                ({0}) AND icode!='LSUMNS'
            ORDER BY 
                sex ASC, year DESC 
            LIMIT 15
            """.format(where), con=connection)
    return other_tissue_df


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
    sheet = pd.read_excel("Genus Sampling Working.xlsx")
    # check to see if these genera/species are in IOC taxonomy
    conf = configparser.ConfigParser(allow_no_value=True)
    conf.optionxform = str
    conf.read(args.species_config)
    species = conf.items('species')
    print("Checking Taxonomy...\n")
    # get dictionaries of starting taxon names + any other taxonomies (for IOC taxa)
    tmp_ioc_taxa, tmp_non_ioc_taxonomy = check_species_list_against_ref_taxonomy(con, sheet)
    # if they are, get all the alt_genus/alt_species names we can
    ioc_taxonomy = {}
    non_ioc_taxonomy = {}
    print("Getting taxonomies for IOC taxa")
    cycled_color = cycle(range(2))
    # create master dataframe for tissues
    master_lsu_tissues = pd.DataFrame()
    master_other_tissues = pd.DataFrame()
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
        # get the lsu tissue db records
        lsu_tissue_records = get_lsu_tissue_records(con, taxa_names)
        # get the overall tissue db records
        other_tissue_records = get_other_tissue_records(con, taxa_names)
        # extract the starting information from the spreadsheet
        sheet_info = sheet.loc[sheet['Genus species'] == taxon]
        if (len(lsu_tissue_records) == 0) and (len(other_tissue_records) == 0):
            master_missing_tissues = master_missing_tissues.append(sheet_info,ignore_index=True)
        # FOR LSU TISSUES
        # duplicate number of rows in sheet_info to equal info from tissue records
        if len(lsu_tissue_records) > 1:
            lsu_sheet_info = pd.concat([sheet_info] * len(lsu_tissue_records))
        else:
            lsu_sheet_info = copy.deepcopy(sheet_info)
        # reset index on sheet_info
        lsu_sheet_info = lsu_sheet_info.reset_index()
        # join these two df
        lsu_tissue_df = pd.concat([lsu_sheet_info, lsu_tissue_records], axis=1, sort=False)
        # add a column indicating which color we'll use for records
        color = next(cycled_color)
        lsu_tissue_df['Color'] = color
        if len(lsu_tissue_records) == 0:
            lsu_tissue_df['Missing'] = True
        else:
            lsu_tissue_df['Missing'] = False
        master_lsu_tissues = master_lsu_tissues.append(lsu_tissue_df,ignore_index=True)
        # FOR OTHER TISSUES
        if len(other_tissue_records) > 1:
            other_sheet_info = pd.concat([sheet_info] * len(other_tissue_records))
        else:
            other_sheet_info = copy.deepcopy(sheet_info)
        # reset index on sheet_info
        other_sheet_info = other_sheet_info.reset_index()
        # join these two df
        other_tissue_df = pd.concat([other_sheet_info, other_tissue_records], axis=1, sort=False)
        # add a column indicating which color we'll use for records
        other_tissue_df['Color'] = color
        if len(other_tissue_records) == 0:
            other_tissue_df['Missing'] = True
        else:
            other_tissue_df['Missing'] = False
        master_other_tissues = master_other_tissues.append(other_tissue_df,ignore_index=True)
    master_lsu_tissues.to_excel('{}-lsu_merged_tissues.xlsx'.format(date.today()))
    master_other_tissues.to_excel('{}-other_merged_tissues.xlsx'.format(date.today()))
    master_missing_tissues.to_excel('{}-missing_tissues.xlsx'.format(date.today()))
    con.close()
    print("\nFinished.\n")