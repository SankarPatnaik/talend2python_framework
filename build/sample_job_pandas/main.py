import argparse
import json
import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    # Allow overriding of input and output file paths via command line.  When
    # generating from a Talend job these values will default to those
    # specified in the job configuration.
    p.add_argument("--input_csv", required=False)
    p.add_argument("--output_csv", required=False)
    return p.parse_args()


def main():
    args = parse_args()
    df_map = {}  # holds intermediate DataFrames keyed by node id
    last_df = None
    # Node: InputCSV (tFileInputDelimited)
    path = args.input_csv or "data/input.csv"
    sep = ","
    header = "true".lower() == "true"
    df = pd.read_csv(path, sep=sep, header=0 if header else None)
    df_map["n1"] = df
    last_df = df
    # Node: Filter (tFilterRow)
    # Apply a row level filter using pandas' query method.  The expression
    # string should be valid Python/pandas syntax referencing column names.
    expr_str = 'age > 18'
    df = last_df.query(expr_str)
    df_map["n2"] = df
    last_df = df
    # Node: SelectRename (tMap)
    # Select and/or compute new columns.  A JSON mapping is provided where
    # keys are output column names and values are either '__keep__', '__copy__'
    # (to preserve the existing column) or an expression to evaluate.
    mapping = json.loads('{"name":"__keep__","age":"__keep__","age_plus_1":"age + 1"}')
    df = pd.DataFrame()
    for new_col, expr_str in mapping.items():
        if expr_str in ["__keep__", "__copy__"]:
            df[new_col] = last_df[new_col]
        else:
            # Use pandas.eval to compute the expression in the context of the
            # existing DataFrame.  This allows simple arithmetic and column
            # references (e.g. 'age + 1').
            df[new_col] = last_df.eval(expr_str)
    df_map["n3"] = df
    last_df = df
    # Node: Preview (tLogRow)
    # Print the first few rows of the current DataFrame.  to_string() is used
    # to avoid truncation of wide columns.
    print(last_df.head(10).to_string())
    df_map["n4"] = last_df
    # Node: OutputCSV (tFileOutputDelimited)
    # Write the DataFrame to a CSV file.  Coerce header and separator
    # parameters to proper boolean and string values.
    out_path = args.output_csv or "build/output.csv"
    sep = ","
    header = "true".lower() == "true"
    last_df.to_csv(out_path, index=False, sep=sep, header=header)
    df_map["n5"] = last_df


if __name__ == "__main__":
    main()