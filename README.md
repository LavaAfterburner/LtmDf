

# LTMDF

A python module to create larger than memory data frames for **pandas**, written in **Cython** for optimal performance.

(**L**arger **T**han **M**emory **D**ata **F**rame)

## ltmdf.Dataframe

### Example:
```python
df = ltmdf.DataFrame()
df.from_csv("Long_Csv.txt", 1000000, ["time", "Value"])
df.add_padding(2)
```
##### from_csv(...)
 1. `"Long_Csv.txt"` Csv File
 2. `1000000` Rows per chunk
 3. `["time", "value"]` Column names
 4. `**kwargs` Kwargs to pass to the pandas read function
 ##### add_padding(...)

 1. `2` How many rows of padding

Creates an environment in the run location, to store the larger than memory Dataframe. After that functions can be run on the entire DF

### Why to use Padding
To avoid NaN when using e.g. `diff` add padding to a DF. It adds the next rows of the df and ignores these after processing

## ltmdf.Environment
Designed for creating an environment for a larger than memory class

## Todo:

 - [x] Add padding to file
 - [ ] Run pandas methods on DF
