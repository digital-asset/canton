#!/usr/bin/env python3


import argparse
import os
import re

pg_exceptions = {
    "V1_2__initial_views.sql" : [
        "returns varchar"
    ],
    "V2_1__lapi_3.0_views.sql" : [
        "returns varchar"
    ]
}

h2_exceptions = {}

def ignore(fname, line, exceptions):
    for check in exceptions.get(fname, []):
        if check in line:
            return True
    return False

def get_parser():
    parser = argparse.ArgumentParser(description = "Check sql definitions for max length and collation")
    parser.add_argument("path", help="Path to sql files", nargs="+")
    parser.add_argument("--type", help="database type", choices=["postgres", "h2"], required=True)
    return parser.parse_args()

def check_statement(dbType, fname, line):
    if dbType == "postgres":
        check_statement_pg(fname, line)
    elif dbType == "h2":
        check_statement_h2(fname, line)
    else:
        raise Exception("unsupported db type " + dbType)

def check_statement_pg(fname, line):
    lline = line.lower()
    if "text" in lline and not ignore(fname, line, pg_exceptions) and re.match(".*\\s+text\\s+\\.*", lline):
        raise Exception("text definition prohibited, please use varchar instead")
    if "varchar" in lline and not ignore(fname, line, pg_exceptions) and re.match(".*\\s+varchar\\(\\d+\\).*", lline):
        raise Exception("fixed length varchar-s are prohibited, please remove the length definition")
    if "varchar" in lline and not ignore(fname, line, pg_exceptions) and not re.match(".*\\s+collate\\s+\"c\".*", lline):
        raise Exception("varchar defined without collate \"C\"")

def check_statement_h2(fname, line):
    lline = line.lower()
    if "text" in lline and not ignore(fname, line, pg_exceptions) and re.match(".*\\s+text\\s+\\.*", lline):
        raise Exception("text definition prohibited, please use varchar instead")
    if "varchar" in lline and not ignore(fname, line, h2_exceptions) and re.match(".*\\s+varchar\\(\\d+\\).*", lline):
        raise Exception("fixed length varchar-s are prohibited")

def descend_tree(dbType, path):
    for e in os.listdir(path):
        fpath = path + '/' + e
        if e[-4:] == ".sql":
            print("reading file", fpath)
            with open(fpath, "r") as f:
                for idx,line in enumerate(f.readlines()):
                    try:
                        check_statement(dbType, e, line)
                    except Exception as e:
                        raise Exception("At %s:%d: %s (%s)" % (fpath, idx + 1, e.args[0], line))
        elif os.path.isdir(fpath):
            descend_tree(dbType, fpath)

if __name__ == "__main__":
    args = get_parser()
    for path in args.path:
        if os.path.exists(path):
            descend_tree(args.type, path)
        else:
            print("skipping", path)

