#!/usr/bin/python3
import sys
import csv
import argparse
import numpy as np
import random
from itertools import islice
import pdb

def GetSchema(file_name):
    infile = open(file_name + ".csv", "r").readlines()
    col2min = {}
    col2max = {}
    metadatas = {}
    col_set = {}
    table_set = set()
    col = []
    
    print("starting read base information...")
    for eachline in islice(infile, 1, None):
        table_set.add(eachline.split(",")[0].split(".")[0])
        col.append(eachline.split(",")[0])
        col2min[eachline.split(",")[0]] = eachline.split(",")[1]
        col2max[eachline.split(",")[0]] = eachline.split(",")[2]
        metadatas[eachline.split(",")[0]] = [eachline.split(",")[3], eachline.split(",")[4]]
    
    for each_table in table_set:
        temp_col = []
        for each_col in col:
            if each_col.split(".")[0] == each_table:
                temp_col.append(each_col)
        col_set[each_table] = temp_col

    print("read completed...")
    return table_set, col_set, col2min, col2max, metadatas


def GenJoin(max_joins, base_table, table_set, col_set):
    join_count = 0
    joins = set()
    tables = set()

    tables.add(base_table)
    while join_count < max_joins:
        first_col = ""
        first_table = random.choice(list(tables))
        second_col = ""
        second_table = ""
        candicate_list = []
    
        while True:
            if first_col != "id" and first_col != "movie_id":
                first_col = random.choice(col_set[first_table]).split(".")[1]
                continue
            else:
                break
        while second_table == "" or second_table in tables:
            second_table = random.choice(list(table_set))
        tables.add(second_table)
        while second_col == "":
            for each in col_set[second_table]:
                if each.split(".")[1] == "id" or each.split(".")[1] == "movie_id":
                    candicate_list.append(each.split(".")[1])
            second_col = random.choice(candicate_list)

        joins.add(first_table + "." + first_col + "=" + second_table + "." + second_col)
        join_count = len(joins)

    return joins, tables

def GenPredicate(tables, width, col_set, metadatas, col2min, col2max):
    pred_count = 0
    predicates = []
    operator = [">", "=", "<", "!=", "<=", ">="]
    log_operator = ["AND", "OR"]
    not_operator = ["NOT", ""]

    while pred_count < width:
        current_col = ""
        current_table = random.choice(list(tables))

        while current_col == "" or metadatas[current_col][0] == metadatas[current_col][1]:
            current_col = random.choice(col_set[current_table])
            op = random.choice(operator)
            log_op = random.choice(log_operator)
            not_op = random.choice(not_operator)
            val = random.randint(int(col2min[current_col]), int(col2max[current_col]))
            val = str(val)

        if pred_count != width - 1:
            if not_op == "":
                predicates.append(current_col + op + val + " " + log_op + " ")
            else:
                predicates.append(not_op + " " + current_col + op + val + " " + log_op + " ")
        else:
            if not_op == "":
                predicates.append(current_col + op + val)
            else:
                predicates.append(not_op + " " + current_col + op + val)

        pred_count += 1

    return predicates


def GenWorkload(file_name, joins, width, queries):
    count = 0
    alias = {}
    Query = set()
    outfile = open("train.sql", "w+")
    table_set, col_set, col2min, col2max, metadatas = GetSchema(file_name)

    infile = open("table.csv", "r")
    for each in islice(infile, 1, None):
        alias[each.split(",")[1].strip("\n")] = each.split(",")[0]

    while count < queries:
        query = "SELECT COUNT(*) FROM "
        current_join = random.randint(0, joins)
        current_width = random.randint(0, width)
        base_table = random.choice(list(table_set))

        if current_join == 0 and current_width == 0:
            query += alias[base_table] + " " + base_table + ";"

        elif current_join != 0 and current_width != 0:
            join_preds, tables = GenJoin(current_join, base_table, table_set, col_set)
            for each_table in tables:
                query += alias[each_table] + " " + each_table + ","
            query = query.rstrip(",") + " "

            query += "WHERE "
            for each_join in join_preds:
                query += each_join + " " + "AND" + " "      
        
            predicates = GenPredicate(tables, current_width, col_set, metadatas, col2min, col2max)
            query += "("
            for each_predicate in predicates:
                query += each_predicate
            query += ")" + ";"

        elif current_join != 0 and current_width == 0:
            join_preds, tables = GenJoin(current_join, base_table, table_set, col_set)
            for each_table in tables:
                query += alias[each_table] + " " + each_table + ","
            query = query.rstrip(",") + " "

            query += "WHERE "
            for each_join in join_preds:
                query += each_join + " " + "AND" + " "
            query = query.rstrip(" ").rstrip("AND").rstrip(" ") + ";"

        else:
            join_preds, tables = GenJoin(current_join, base_table, table_set, col_set)
            for each_table in tables:
                query += alias[each_table] + " " + each_table + ","
            query = query.rstrip(",") + " "

            query += "WHERE "
            predicates = GenPredicate(tables, current_width, col_set, metadatas, col2min, col2max)
            
            for each_predicate in predicates:
                query += each_predicate
            query += ";"

        Query.add(query)
        count = len(Query)

    for each_query in Query:
        outfile.write(each_query)
        outfile.write("\n")
    print("trainning set are generated")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="file path of metadata")
    parser.add_argument("--joins", help="number of needing joins (default: 2)", type=int, default=2)
    parser.add_argument("--width", help="number of predicates width (default: 5)", type=int, default=2)
    parser.add_argument("--queries", help="total queries (default: 1000)", type=int, default=100000)
    args = parser.parse_args()
    GenWorkload(args.filename, args.joins, args.width, args.queries)

if __name__ == "__main__":
    main()
