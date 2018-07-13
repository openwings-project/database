#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
(c) 06/13/18 Brant Faircloth || http://faircloth-lab.org/
All rights reserved.

This code is distributed under a 3-clause BSD license. Please see
LICENSE.txt for more information.

Created on Jun 13, 2018 at 09:50:25.
"""

import os
import sys
import argparse
import configparser
from datetime import date

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
        help="""The directory containing alignments to be screened."""
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


def check_species_list_against_ref_taxonomy(connection, conf):
    taxa = []
    for taxon in conf.items('species'):
        genus, species = taxon[0].split(' ')
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
        except AssertionError:
            print("{}\tis not in the IOC taxonomy".format(taxon[0]))
        for row in query:
            taxa.append(row)
    return taxa


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
    # check to ensure that species exist in the db
    conf = configparser.ConfigParser(allow_no_value=True)
    conf.optionxform = str
    conf.read(args.species_config)
    species = conf.items('species')
    print("Checking Taxonomy...\n")
    taxa = check_species_list_against_ref_taxonomy(con, conf)
    museums = ('v_amnh', 'v_lsumns', 'v_ku', 'v_fmnh', 'v_usnm', 'v_uwbm', 'v_ala', 'v_vertnet')
    ref_species = get_temp_table_of_reference_taxonomy_species(con, taxa)
    species_only = ref_species.copy(deep=True)
    # holder for museum-specific results
    all_museum_results = {}
    # query information by family
    for museum in museums:
        tax_names = [museum, ]
        # create empty data frame for a given museum
        holdings_ref_species = species_only.copy(deep=True)
        # get the standard IOC taxonomy holdings
        holdings_species = get_holdings_species(con, museum)
        if holdings_species.empty:
            holdings_ref_species[museum] = numpy.NaN
        else:
            holdings_species[museum] = True
        holdings_ref_species = holdings_ref_species.merge(holdings_species, left_on=['genus','species'], right_on=['genus','species'], how='outer')
        # now that we've done that, add in records across different taxonomies
        for taxonomy in (1,2,3,4,5,6,7):
            holdings_species_by_tax = get_holdings_species_by_taxonomy(con, museum, taxonomy)
            if holdings_species_by_tax.empty:
                holdings_species_by_tax = species_only.copy(deep=True)
                holdings_species_by_tax["{}_tax{}".format(museum, taxonomy)] = numpy.NaN
            else:
                holdings_species_by_tax["{}_tax{}".format(museum, taxonomy)] = True
            holdings_ref_species = holdings_ref_species.merge(holdings_species_by_tax, left_on=['genus','species'], right_on=['genus','species'], how='outer')
            tax_names.append("{}_tax{}".format(museum, taxonomy))
        # sum across True or False values
        holdings_ref_species["present_{}".format(museum)] = holdings_ref_species[tax_names].sum(axis=1)
        # just get subset of genus, species, present
        temp = holdings_ref_species[['genus','species', "present_{}".format(museum)]].copy(deep=True)
        # convert values > 1.0 to "True"
        temp["present_{}".format(museum)] = temp["present_{}".format(museum)].apply(lambda x: True if x > 0.0 else False)
        # replace 1 and 0 with True and False
        ref_species = ref_species.merge(temp, left_on=['genus','species'], right_on=['genus','species'], how='outer')
    #pdb.set_trace()
    holdings_total_names = ["present_{}".format(museum) for museum in museums]
    ref_species["all_museums_totals"] = ref_species[holdings_total_names].sum(axis=1)
    ref_species["all_collab_museums"] = ref_species[holdings_total_names[:6]].sum(axis=1)
    # just get subset of genus, species, present
    sum_species = ref_species[['genus','species', "all_collab_museums", "all_museums_totals"]].copy(deep=True)
    sum_species["all_museums_totals"] = sum_species["all_museums_totals"].apply(lambda x: True if x > 0.0 else False)
    sum_species["all_collab_museums"] = sum_species["all_collab_museums"].apply(lambda x: True if x > 0.0 else False)
    # sort the ref_species list
    ref_species.sort_values(['genus','species'], inplace=True)
    sum_species.sort_values(['genus','species'], inplace=True)
    writer = pd.ExcelWriter('institutional_holdings_by_SPECIFIC_species_{0}.xlsx'.format(date.today()))
    ref_species.to_excel(writer,'Totals by Museum')
    sum_species.to_excel(writer,'Summary')
    writer.save()
    con.close()
    print("\nFinished.\n")