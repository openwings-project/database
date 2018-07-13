#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
(c) 06/12/18 Brant Faircloth || http://faircloth-lab.org/
All rights reserved.

This code is distributed under a 3-clause BSD license. Please see
LICENSE.txt for more information.

Created on Jun 12, 2018 at 14:07:11.
"""

import sys
from datetime import date
import pandas as pd
from sqlalchemy import create_engine

import pdb


def get_holdings_species_counts(connection, museum, family):
    df = pd.read_sql_query('''
        SELECT
            species.genus,
            species.species,
            count(*) as {0}
        FROM
            {0},
            species
        WHERE
            species.family='{1}'
            AND species.genus={0}.genus
            AND species.species={0}.species
        GROUP BY
            species.genus, species.species;
        '''.format(museum, family), con=connection)
    return df


def get_reference_taxonomy_families(connection):
    df = pd.read_sql_query('''
        SELECT
            species.family,
            count(DISTINCT (species.genus, species.species))
        FROM
            species
        GROUP BY
            species.family;
        ''', con=connection)
    return df


def get_reference_taxonomy_species(connection, family):
    df = pd.read_sql_query('''
        SELECT
            species.genus, species.species
        FROM
            species
        WHERE
            species.family='{0}';
        '''.format(family), con=connection)
    return df


if __name__ == '__main__':
    print("Starting.\n")
    db_conf = configparser.ConfigParser()
    db_conf.read("access.conf")
    connection_string = "postgresql://{0}:{1}@localhost:5432/openwings".format(
            db_conf['openwings']['user'],
            db_conf['openwings']['password']
        )
    engine = create_engine(connection_string)
    con = engine.connect()
    ref_families = get_reference_taxonomy_families(con)
    # get information by family
    for family in ref_families.iterrows():
        print(family[1].family.upper())
        # get the counts for the reference_tax
        ref_species = get_reference_taxonomy_species(con, family[1].family)
        species_only = ref_species.copy(deep=True)
        # query information by family
        for museum in ('v_amnh', 'v_lsumns', 'v_ku', 'v_fmnh', 'v_usnm', 'v_uwbm', 'v_ala', 'v_vertnet'):
            print("\t{0}".format(museum))
            holdings_species = get_holdings_species_counts(con, museum, family[1].family)
            # with species records, it's possible we get null returns.
            # we need to do something if we get these totally empty results
            # so, copy the ref dataframe and fill with zeros
            if holdings_species.empty:
                holdings_species = species_only.copy(deep=True)
                holdings_species[museum] = 0
            ref_species = ref_species.merge(holdings_species, left_on=['genus','species'], right_on=['genus','species'], how='outer')
        ref_species.to_csv('institutional_holdings_by_species_{0}_{1}.csv'.format(date.today(), family[1].family.upper()), header=True)
        #pdb.set_trace()
    print("\nFinished.\n")