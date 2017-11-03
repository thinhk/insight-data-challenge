# insight-data-challenge

Thank you for the opportunity to participate in this challenge! Also thanks for the extension because I was on vacation!

# Dependencies
The external libraries used in this challenge are:
* `pandas`
* `numpy` 

Other packages used in this are included in python.
* `heapq`
* `sys`

# Explanation
I tackled this coding challenge in four parts:
### Streaming in the data

When streaming the data, I chose to approach the challenge with a python library that I was familiar 
with - pandas. I took into consideration the capabilities of pandas and saw that it was suitable for 
this challenge. With some research, I saw that pandas is faster than the base csv module in python. 
Also i saw that pandas can chunk the incoming data, which makes the reading data in more efficient 
than reading one line at a time.

Pandas also offered plenty of useful tools that helps with data ingestion and manipulation. 
Since I've worked with databases a lot, the ability to specify a schema in pandas made manipulating 
data a lot easier. Also, the dataframe object has similar implementations in more scalable libraries
like dask or RDDs in Spark.


### Validating the data

Validating data came in two steps, one was an overarching filter in the beginning to 
filter `OTHER_ID`, and missing `TRANSACTION_AMT`/`CMTE_ID`.

The second step for validating data was to look for when the `ZIP_CODE` was malformed 
and only aggregate dates, and when `TRANSACTION_AMT` was malformed and only aggregate zips.

### Aggregating the data

Aggregating the data came in the form of using dictionaries and (`CMTE_ID`,`ZIP_CODE`) 
and (`CMTE_ID`,`TRANSACTION_DT`') tuples as the keys.
The easy part was counting transactions and summing up transaction amounts. The tricky 
part of the challenge was creating a streaming median for the `medianvals_by_zip.txt`.

I chose to use a heap method to calculate the median because this method requires 
the least sorting and should be the best for scalability. I thought about just adding
`TRANSACTION_AMT` to a list and running a median function on it, but the sorting
seems like it would increase the run time.


### Exporting/printing the data

Exporting the data for the `medianvals_by_zip.txt` portion was a constant insert into a text 
file as the data was streamed in. Every line that was streamed in was aggregated, and the 
aggregated result was printed as of that line.

Exporting the data for the `medianvals_by_date.txt` portion was just to stream the whole file 
and aggregate it, and then print the final dictionary when the stream ended.

# Testing
For testing, I extracted subsets from the base data:
* `error_date` - malformed `TRANSACTION_DT` subset, and tested to see if it produced an empty `medianvals_by_date.txt` file.  
* `error_zip` - malformed `ZIP_CODE` subset, and tested to see if it produced an empty `medianvals_by_zip.txt` file.
* `error_other` - contains `OTHER_ID` or empty `CMTE_ID`/`TRANSACTION_AMT`, should produce both empty files.

