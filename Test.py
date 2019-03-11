import ltmdf
import time


def process(df):
    print(df)
    return df


def x():
    df = ltmdf.DataFrame()
    df.from_csv(".\\DUMP.TXT", 100000, ["A", "B", "C"])
    df.run(process)

    df2 = ltmdf.DataFrame()
    df2.from_ltmdf(df)

    time.sleep(3)


x()


"""
ltmdf.read_file(".\\DUMP.TXT", 1000000, ["A", "B", "C"])
ltmdf.for_df(process)
"""
