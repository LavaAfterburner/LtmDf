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
"""
with ltmdf.DataFrame(name="MyDf", preserved=True, zipped=True) as x:
    # x = ltmdf.DataFrame(preserved=True)
    x.from_csv(".\\DUMP SMALL.TXT", 200000, ["A", "B", "C"])
    x.add_padding(1)
    x.run(process)
    x.run(print_)
"""
with ltmdf.DataFrame(name="MyDf.zip", preserved=True, zipped=True, load_env=True) as x:
    print(x.__dict__)
    x.run(print_)
