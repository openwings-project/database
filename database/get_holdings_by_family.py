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


def get_holdings_genera_counts(connection, museum, family):
    df = pd.read_sql_query('''
        SELECT
            species.genus,
            count(DISTINCT (species.genus, species.species)) as {0}
        FROM
            species,
            {0}
        WHERE
            species.family='{1}'
            AND species.genus={0}.genus
            AND species.species={0}.species
        GROUP BY
            species.genus;
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


def get_reference_taxonomy_genera(connection, family):
    df = pd.read_sql_query('''
        SELECT
            species.genus,
            count(DISTINCT (species.genus, species.species))
        FROM
            species
        WHERE
            species.family='{0}'
        GROUP BY
            species.genus;
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
        ref_genera = get_reference_taxonomy_genera(con, family[1].family)
        # query information by family
        for museum in ('v_amnh', 'v_lsumns', 'v_ku', 'v_fmnh', 'v_usnm', 'v_uwbm', 'v_ala', 'v_vertnet'):
            holdings_genera = get_holdings_genera_counts(con, museum, family[1].family)
            ref_genera = ref_genera.merge(holdings_genera, left_on='genus', right_on='genus', how='outer')
            print("\t{0}".format(museum))
        ref_genera.to_csv('institutional_holdings_by_genus_{0}_{1}.csv'.format(date.today(), family[1].family.upper()), header=True)
    print("\nFinished.\n")