#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
(c) 06/13/18 Brant Faircloth || http://faircloth-lab.org/
All rights reserved.

This code is distributed under a 3-clause BSD license. Please see
LICENSE.txt for more information.

Created on Jun 13, 2018 at 09:50:25.
"""

import sys
from datetime import date
import pandas as pd
import numpy
from sqlalchemy import create_engine

import pdb


def get_holdings_species(connection, museum, family):
    df = pd.read_sql_query('''
        SELECT
            species.genus,
            species.species
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


def get_holdings_species_by_taxonomy(connection, museum, family, taxonomy):
    df = pd.read_sql_query("""
        SELECT
            species.genus,
            species.species
        FROM
            {0},
            species,
            taxonomies
        WHERE
            species.family='{1}'
            AND taxonomies.taxonomy_id={2}
            AND species.genus = taxonomies.genus
            AND species.species = taxonomies.species
            AND (species.genus != taxonomies.alt_genus OR species.species != taxonomies.alt_species)
            AND {0}.genus = taxonomies.alt_genus
            AND {0}.species = taxonomies.alt_species
        GROUP BY
            species.genus, species.species;
        """.format(museum, family, taxonomy), con=connection)
    return df


def get_reference_taxonomy_families(connection):
    df = pd.read_sql_query("""
        SELECT
            family.name
        FROM
            family
        ORDER BY
            family.name;
        """, con=connection)
    return df


def get_reference_taxonomy_species(connection, family):
    df = pd.read_sql_query("""
        SELECT
            species.genus, species.species
        FROM
            species
        WHERE
            species.family='{0}';
        """.format(family), con=connection)
    return df


if __name__ == '__main__':
    print("Starting.\n\n")
    print("Family,Species,Others,Ours,MissingOurs,MissingOthers")
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
    museums = ('v_amnh', 'v_lsumns', 'v_ku', 'v_fmnh', 'v_usnm', 'v_uwbm', 'v_ala', 'v_vertnet')
    for family in ref_families.iterrows():
        # get the counts for the reference_tax
        ref_species = get_reference_taxonomy_species(con, family[1][0])
        species_only = ref_species.copy(deep=True)
        # holder for museum-specific results
        all_museum_results = {}
        # query information by family
        for museum in museums:
            tax_names = [museum, ]
            # create empty data frame for a given museum
            holdings_ref_species = species_only.copy(deep=True)
            # get the standard IOC taxonomy holdings
            holdings_species = get_holdings_species(con, museum, family[1][0])
            if holdings_species.empty:
                holdings_ref_species[museum] = numpy.NaN
            else:
                holdings_species[museum] = True
            holdings_ref_species = holdings_ref_species.merge(holdings_species, left_on=['genus','species'], right_on=['genus','species'], how='outer')
            # now that we've done that, add in records across different taxonomies
            for taxonomy in (1,2,3,4,5,6,7):
                holdings_species_by_tax = get_holdings_species_by_taxonomy(con, museum, family[1][0], taxonomy)
                if holdings_species_by_tax.empty:
                    holdings_species_by_tax = species_only.copy(deep=True)
                    holdings_species_by_tax["{}_tax{}".format(museum, taxonomy)] = numpy.NaN
                else:
                    holdings_species_by_tax["{}_tax{}".format(museum, taxonomy)] = True
                holdings_ref_species = holdings_ref_species.merge(holdings_species_by_tax, left_on=['genus','species'], right_on=['genus','species'], how='outer')
                tax_names.append("{}_tax{}".format(museum, taxonomy))
            #pdb.set_trace()
            # sum across True or False values
            holdings_ref_species["present_{}".format(museum)] = holdings_ref_species[tax_names].sum(axis=1)
            # just get subset of genus, species, present
            temp = holdings_ref_species[['genus','species', "present_{}".format(museum)]].copy(deep=True)
            # convert values > 1.0 to "True"
            temp["present_{}".format(museum)] = temp["present_{}".format(museum)].apply(lambda x: True if x > 0.0 else False)
            # replace 1 and 0 with True and False
            ref_species = ref_species.merge(temp, left_on=['genus','species'], right_on=['genus','species'], how='outer')
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
        writer = pd.ExcelWriter('institutional_holdings_by_species_{0}_{1}.xlsx'.format(date.today(), family[1][0].upper()))
        ref_species.to_excel(writer,'Totals by Museum')
        sum_species.to_excel(writer,'Summary')
        writer.save()
        #pdb.set_trace()
        print("{},{},{},{},{},{}".format(
            family[1][0],
            len(ref_species),
            sum_species["all_museums_totals"].sum(axis=0),
            sum_species["all_collab_museums"].sum(axis=0),
            len(ref_species) - sum_species["all_collab_museums"].sum(axis=0),
            len(ref_species) - sum_species["all_museums_totals"].sum(axis=0)
        ))
        #pdb.set_trace()
    print("\nFinished.\n")