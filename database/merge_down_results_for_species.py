#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
(c) 2018 Brant Faircloth || http://faircloth-lab.org/

All rights reserved.

This code is distributed under a 3-clause BSD license. Please see
LICENSE.txt for more information.

Created on 13 July 2018 10:46 CDT (-0500)
"""

import os
import glob
import shutil
import argparse

import pandas as pd

import pdb

class FullPaths(argparse.Action):
    """Expand user- and relative-paths"""
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, os.path.abspath(os.path.expanduser(values)))


class CreateDir(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        # get the full path
        d = os.path.abspath(os.path.expanduser(values))
        # check to see if directory exists
        if os.path.exists(d):
            answer = input("[WARNING] Output directory exists, REMOVE [Y/n]? ")
            if answer == "Y":
                shutil.rmtree(d)
            else:
                print("[QUIT]")
                sys.exit()
        # create the new directory
        os.makedirs(d)
        # return the full path
        setattr(namespace, self.dest, d)


def is_dir(dirname):
    if not os.path.isdir(dirname):
        msg = "{0} is not a directory".format(dirname)
        raise argparse.ArgumentTypeError(msg)
    else:
        return dirname


def get_args():
    parser = argparse.ArgumentParser(
        description="""Given a config file of species, return db records""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--species-files',
        required=True,
        type=is_dir,
        action=FullPaths,
        help="""The directory containing species record spreadsheets to merge down."""
    )
    parser.add_argument(
        '--output',
        required=True,
        action=CreateDir,
        help="""The directory in which to write the results."""
    )
    return parser.parse_args()

def main():
    args = get_args()
    files = glob.glob(os.path.join(args.species_files, "*.xlsx"))
    frames = []
    for file in files:
        name = os.path.basename(file)
        sname = os.path.splitext(name)
        order = sname[0].split("_")[-1]
        print("Working on {}".format(name))
        new_name = "{}.no-tissues{}".format(sname[0], sname[1])
        excel = pd.read_excel(file)
        no_tissues = excel[excel.all_collab_museums == 0]
        out_path = os.path.join(args.output, new_name)
        writer = pd.ExcelWriter(out_path)
        no_tissues.to_excel(writer,'Totals by Museum')
        no_tissues.insert(0, 'family', order)
        #pdb.set_trace()
        frames.append(no_tissues)
        writer.save()
    # now, lets write one file that contains all of the data
    concatenated = pd.concat(frames)
    out_path2 = os.path.join(args.output, "ALL-TAXA-MISSING-TISSUES.xlsx")
    writer = pd.ExcelWriter(out_path2)
    concatenated.to_excel(writer,'Totals by Museum')
    writer.save()
    #pdb.set_trace()


if __name__ == '__main__':
    main()