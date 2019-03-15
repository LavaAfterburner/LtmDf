import ltmdf
import time


def process(df):
    df["DT"] = df["B"].diff()
    return df


def print_(df):
    print(df)
    return df

"""
def x():
    df = ltmdf.DataFrame()
    df.from_csv(".\\DUMP.TXT", 100000, ["A", "B", "C"])
    df.run(process)
    df.run(print_)


x()
"""

x = ltmdf.DataFrame()
x.from_csv(".\\DUMP.TXT", 100000, ["A", "B", "C"])
x.add_padding(5)
x.run(process)
x.run(print_)
