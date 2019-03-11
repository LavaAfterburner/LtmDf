#!python
#cython: boundscheck=False
#cython: wraparound=False


import pandas as pd
import pickle
import os
import shutil


class DataFrame:
    def run(self, func, list args=[], dict kwargs={}):
        cdef str chunk_name
        cdef df
        for chunk_name in self.chunks:
            with open(chunk_name, "rb") as file:
                df = pickle.load(file)
                df = func(df, *args, **kwargs)
            with open(chunk_name, "wb") as file:
                pickle.dump(df, file)

    def from_csv(self, str path, int chunksize, list columns, **kwargs):
        cdef int num
        cdef str chunk_name
        for num, chunk in enumerate(pd.read_csv(
                                                path,
                                                chunksize=chunksize,
                                                **kwargs)):
            chunk_name = self.env + "df" + str(num) + ".dfc"
            with open(chunk_name, "wb") as file:
                chunk.columns = columns
                pickle.dump(chunk, file)
                self.chunks.append(chunk_name)

    def from_pandas(self, df, int chunksize):
        cdef int num
        cdef str chunk_name
        cdef int number_of_chunks = len(df) // chunksize + 1
        for num in range(number_of_chunks):
            chunk_name = self.env + "df" + str(num) + ".dfc"
            with open(chunk_name, "wb") as file:
                chunk = df[num * chunksize: (num + 1) * chunksize]
                pickle.dump(chunk, file)
                self.chunks.append(chunk_name)

    def from_ltmdf(self, df):
        cdef str chunk
        for chunk in df.chunks:
            shutil.copy(chunk, self.env)

    def clear(self):
        cdef file
        for file in os.scandir(self.env):
            os.unlink(file.path)

    def __init__(self):
        self.chunks = list()
        self.env = ".\\{}[LTM-DF]\\".format(_get_dfs_in_dir())

        os.makedirs(self.env)

    def __del__(self):
        shutil.rmtree(self.env)


cdef int _get_dfs_in_dir():
    cdef entry
    cdef int count = 0
    for entry in os.scandir("."):
        if entry.name.endswith("[LTM-DF]"):
            count += 1
    return count
