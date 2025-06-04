"""Select, modify, and prepare variables from SOEP data.

The package takes a SOEP wave of `.dta` data files and returns a merged dataset
containing selected variables. Processing steps include initially cleaning and
assignment of adequate variable names, types, and values.
Further, variables are derived either from existing cleaned variables or by
merging from multiple data files.
Metadata is generated for each variable, including the corresponding file name
and variable data type.
There exists an example of merging variables from multiple data files to create
a final dataset.

For a more thorough understanding generate an image of the Directed Acyclic Graph
(DAG) of the tasks in this package using
[`pytask dag`]
(https://pytask-dev.readthedocs.io/en/stable/tutorials/visualizing_the_dag.html).
"""
