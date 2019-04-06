#!python
#cython: boundscheck=False
#cython: wraparound=False


from __future__ import print_function
import pandas as pd
import pickle
import os
import shutil


class Environment:

    SUPPORT = 0
    MAIN = 1

    def __init__(self, name="", preserved=False, zipped=False, load_env=False):
        self.env = self
        self.env_name = name
        self.env_count = 0
        self.env_main_files = list()
        self.env_support_files = dict()
        self.env_is_preserved = preserved
        self.env_is_zipped = zipped

        if not load_env:
            self.create_env(name)
        else:
            self.load_env(name)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()

    def cleanup(self):
        if not self.env_is_preserved:
            self.destroy_env()
        else:
            self.save_env_data()
        if self.env_is_zipped:
            self.zip_env()
            self.destroy_env()

    def __del__(self):
        pass
        #self.cleanup()

    def __getitem__(self, index):
        cdef int type = index[1]
        cdef int target = index[0]
        if type == Environment.MAIN:
            # Is a main file
            return self.env_main_files[target]
        else:
            # Is a support file
            return self.env_support_files[target]

    def add_main_file(self, file, mode="wb"):
        cdef str name = self.next_file_name()
        self.env_main_files.append(name)
        self.env_count += 1

        with open(name, mode) as file_out:
            self.to_file(file, file_out)
        return len(self.env_main_files) - 1

    def add_support_file(self, parent, name, file, mode="wb"):
        if parent not in self.env_support_files:
            self.env_support_files[parent] = dict()

        cdef str file_name = self.next_support_name()
        self.env_support_files[parent][name] = file_name
        self.env_count += 1
        with open(file_name, mode) as file_out:
            self.to_file(file, file_out)

        return (parent, name)

    def add_support_reference(self, parent, name, ref):
        if parent not in self.env_support_files:
            self.env_support_files[parent] = dict()

        self.env_support_files[parent][name] = \
            self[ref[0], Environment.SUPPORT][ref[1]]

    def next_env_name(self):
        cdef dir
        cdef count = 0
        for dir in os.scandir(".\\"):
            if dir.name.endswith("[LTM-DF]"):
                count += 1
        return "{}[LTM-DF]".format(count)

    def next_file_name(self):
        return "{}\\{}[M]".format(self.env_name, self.env_count)

    def next_support_name(self):
        return "{}\\{}[S]".format(self.env_name, self.env_count)

    def create_env(self, str name):
        if name == "":
            self.env_name = self.next_env_name()
        else:
            self.env_name = name
        os.makedirs(self.env_name)

    def destroy_env(self):
        shutil.rmtree(self.env_name)

    def save_env_data(self):
        with open("{}\\ltmdf".format(self.env_name), mode="wb") as file_out:
            self.to_file(self.__dict__, file_out)

    def zip_env(self):
        shutil.make_archive("{}".format(self.env_name), "zip", self.env_name)

    def unzip_env(self):
        shutil.unpack_archive(self.env_name, self.env_name.strip(".zip"), "zip")
        os.remove(self.env_name)
        self.env_name = self.env_name.strip(".zip")

    def load_env(self, name):
        if name.endswith(".zip"):
            self.unzip_env()

        with open("{}\\ltmdf".format(self.env_name), mode="rb") as file_in:
            self.__dict__.clear()
            self.__dict__.update(self.from_file(file_in))
            self.env = self

    def to_file(self, file, file_out):
        pickle.dump(file, file_out)

    def from_file(self, file_in):
        return pickle.load(file_in)

    def each_main_file(self):
        for file in range(len(self.env_main_files)):
            yield file


class DataFrame(Environment):
    def __init__(self, **env_arguments):
        super().__init__(**env_arguments)
        self.is_padded = False
        self.padding = 0

    def run(self, func, log_progess=False, *args, **kwargs):

        cdef str pad_pre_name, chunk_name, pad_post_name, progress
        cdef pad_pre, chunk, pad_post
        cdef orig_pad_pre, orig_pad_post

        for file in self.each_main_file():

            if log_progess:
                progess = "[{}/{}]".format(file + 1, len(self.env_main_files))
                print(progess + "   ", end="\r")

            chunk_name = self.env[file, Environment.MAIN]
            chunk = self.load_df(chunk_name)
            if log_progess:
                print(progess + ".  ", end="\r")

            if self.is_padded:
                if file != 0:
                    pad_pre_name = self.env[file, Environment.SUPPORT]["pre"]
                    orig_pad_pre = self.load_df(pad_pre_name)
                    pad_pre = orig_pad_pre
                    chunk = pd.concat([pad_pre, chunk], sort=False)

                if file != len(self.env_main_files) - 1:
                    pad_post_name = self.env[file, Environment.SUPPORT]["post"]
                    orig_pad_post = self.load_df(pad_post_name)
                    pad_post = orig_pad_post
                    chunk = pd.concat([chunk, pad_post], sort=False)

            if log_progess:
                print(progess + ".. ", end="\r")
            chunk = func(chunk, *args, **kwargs)

            if log_progess:
                print(progess + "...", end="\r")
            if not chunk is None:
                if self.is_padded:
                    if file != len(self.env_main_files) - 1:
                        pad_post = pd.concat([
                                           chunk.tail(2 * self.padding)
                                               .head(self.padding),
                                           orig_pad_post.tail(self.padding)],
                                           sort=False)
                        self.save_df(pad_post_name, pad_post)
                        chunk.drop(chunk.tail(self.padding).index, inplace=True)

                    if file != 0:
                        pad_pre = pd.concat([
                                          orig_pad_pre.head(self.padding),
                                          chunk.head(2 * self.padding)
                                              .tail(self.padding)],
                                          sort=False)
                        self.save_df(pad_pre_name, pad_pre)
                        chunk.drop(chunk.head(self.padding).index, inplace=True)
                self.save_df(chunk_name, chunk)

    def load_df(self, str file_name):
        cdef df
        with open(file_name, "rb") as file:
            df = pickle.load(file)
        return df

    def save_df(self, str file_name, df):
        with open(file_name, "wb") as file:
            pickle.dump(df, file)

    def from_csv(self, str path, int chunk_len, columns, **kwargs):
        for chunk in pd.read_csv(
                                      path,
                                      chunksize=chunk_len,
                                      **kwargs):
            chunk.columns = columns
            self.add_main_file(chunk)

    def add_padding(self, int padding):
        cdef int file
        cdef pad, chunk
        cdef str chunk_name
        cdef tuple ref

        self.is_padded = True
        self.padding = padding

        for file in self.each_main_file():
            chunk_name = self.env[file, Environment.MAIN]
            chunk = self.load_df(chunk_name)

            if file != 0:
                pad = pd.concat([pad, chunk.head(padding)], sort=False)
                ref = self.add_support_file(file, "pre", pad)
                self.add_support_reference(file - 1, "post", ref)
            pad = chunk.tail(padding)

            if file != 0:
                chunk.drop(chunk.head(padding).index, inplace=True)
            if file != len(self.env_main_files) - 1:
                chunk.drop(chunk.tail(padding).index, inplace=True)
            self.save_df(chunk_name, chunk)

    def no_padding(self, df):
        df.drop(df.head(self.padding / 2).index, inplace=True)
        df.drop(df.tail(self.padding / 2).index, inplace=True)
        return df

    def print_chunks(self):
        print("Printing")
        cdef file

        cdef str pad_pre_name, chunk_name, pad_post_name
        cdef pad_pre, chunk, pad_post

        for file in self.each_main_file():
            print(file, self.env[file, Environment.SUPPORT])
            if file != 0:
                pad_pre_name = self.env[file, Environment.SUPPORT]["pre"]
                pad_pre = self.load_df(pad_pre_name)
                print("PRE")
                print(pad_pre)

            if file != len(self.env_main_files) - 1:
                pad_post_name = self.env[file, Environment.SUPPORT]["post"]
                pad_post = self.load_df(pad_post_name)
                print("POST")
                print(pad_post)

            chunk_name = self.env[file, Environment.MAIN]
            chunk = self.load_df(chunk_name)

            print("=============================")
