# CE_for_Complex_Predicates
A generator for generating complex predicates in SQLs with combination of AND, OR, NOT

using the generating rules in [1] to write this SQL generator demo.
column_min_max_vals.csv includes metadata given by authors in [1].

##usage
For generating 100000 queries,you can input following example command:

'''python3 gen_workload.py column_min_max_vals'''

References:
[1] [Kipf et al., Learned Cardinalities: Estimating Correlated Joins with Deep Learning, 2018]

