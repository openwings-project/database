#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
(c) 06/12/18 Brant Faircloth || http://faircloth-lab.org/
All rights reserved.

This code is distributed under a 3-clause BSD license. Please see
LICENSE.txt for more information.

Created on Jun 12, 2018 at 11:10:31.
"""

import sys
from datetime import date
import pandas as pd
from sqlalchemy import create_engine

import pdb

def get_holdings_family_counts(connection, museum):
    df = pd.read_sql_query('''
        SELECT
            species.family,
            count(DISTINCT (species.genus, species.species)) as {0}
        FROM
            species,
            {0}
        WHERE
            species.genus = {0}.genus
            AND species.species = {0}.species
        GROUP BY
            species.family;
        '''.format(museum), con=connection)
    return df

def get_reference_taxonomy_families(connection):
    df = pd.read_sql_query('''
        SELECT
            species.ordr,
            species.family,
            count(DISTINCT (species.genus, species.species))
        FROM
            species
        GROUP BY
            species.ordr,
            species.family;
        ''', con=connection)
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
    for museum in ('v_amnh', 'v_lsumns', 'v_ku', 'v_fmnh', 'v_usnm', 'v_uwbm', 'v_ala', 'v_vertnet'):
        museum_df = get_holdings_family_counts(con, museum)
        ref_families = ref_families.merge(museum_df, left_on='family', right_on='family', how='outer')
        sys.stdout.write("{}..".format(museum))
        sys.stdout.flush()
    print("\nFinished.\n")
    ref_families.to_csv('institutional_holdings_by_family_{}.csv'.format(date.today()), header=True)

