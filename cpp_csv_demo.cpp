/* Producing descriptive statistics, including pivot table-type data,
using Vince's CSV Parser and Hossein Moein's DataFrame library
(A work in progress)

By Kenneth Burchfiel
Released under the MIT License

Source for aviation data:
Air Carriers : T-100 Segment (All Carriers) table
from the Bureau of Transportation Statistics
https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FMG&QO_
fu146_anzr=Nv4+Pn44vr45

Timeframe used: 2024 (all months)

To get your own copy of this dataset, go to the above URL; specify
2024 as the filter year and 'All months' as the filter period;
Select all tables (or just the ones specified within this script);
and then save it to a .csv file on your computer.
Also make sure to replace the path shown within the CSVReader reader()
call below with your own file path.


// This project's GitHub repo also includes a smaller .csv file that 
// contains only the top 10,000 rows (by passengers) within this table.
// Because the original file is 171 MB in size, I couldn't include
// it within my repository.

Much of the following data import code was based on the following sources:
https://github.com/vincentlaucsb/csv-parser
https://vincela.com/csv/classcsv_1_1CSVRow.html */

// Future updates to implement:
// 1. Show how to filter and sort the DataFrame
// 2. Import datasets for other years also (and create comparisons between them)
// 3. Add code that shows the runtime for this file
// 4. Create equivalent code within Python (using Pandas and/or Polars)
// so that you can see how much processing time (and, perhaps, RAM) 
// this approach saves
// Add more documentation

# include <iostream>
# include "csv.hpp" 
# include <string>
# include <map>
# include <numeric> // for std::accumulate
#include <DataFrame/DataFrame.h>


using namespace csv;
using namespace hmdf;

// Creating a vector that can store all rows within memory:
// (See https://vincela.com/csv/classcsv_1_1CSVRow.html)
// I could have written the code without this component, but 
// I wanted to make sure that I could access fields by their
// index number--for which this vector will be very helpful.

int main() {

/* Sample file (for testing and debugging):
Because I'm building the final program within a /build folder, 
I needed to precede the filename with ../ so that the program 
would know to go up one level to access the file. */
// CSVReader reader("../T_T100_SEGMENT_ALL_CARRIER_2024_top_10k_rows_by_pax.csv");

//Full 2024 file: (stored separately due to its size)
CSVReader reader("/home/kjb3/D1V1/Documents/Large_Datasets/BTS/T_\
T100_SEGMENT_ALL_CARRIER_2024.csv");

// Creating vectors that can store data for selected columns:
// (This approach allows us to only (1) read in data and (2) specify
//  names and types for certain columns,
// which can be useful when working with datasets that have hundreds of fields.)

std::vector<std::string> unique_carrier_col = {};
std::vector<std::string> origin_col = {};
std::vector<double> passengers_col = {};
std::vector<double> distance_col = {};
std::vector<double> departures_performed_col = {};

int row_count = 0;
for (auto& row: reader){
unique_carrier_col.push_back(row["UNIQUE_CARRIER"].get<std::string>());
origin_col.push_back(row["ORIGIN"].get<std::string>());
passengers_col.push_back(row["PASSENGERS"].get<double>());
distance_col.push_back(row["DISTANCE"].get<double>());
departures_performed_col.push_back(row["DEPARTURES_PERFORMED"].get<double>());
row_count += 1;
};

std::cout << "Finished importing two rows. Total row count: " << row_count;

//Defining an index that can be used for our DataFrame:
/* Based on https://htmlpreview.github.io/?https://github.com/
hosseinmoein/DataFrame/blob/master/docs/HTML/load_index.html */
std::vector<unsigned long> bts_index_col = {};
//Defining the row count of our table:
for (int i = 0; i < row_count; ++i) {
    bts_index_col.push_back(i);}

// Importing this data into a DataFrame:
// The Hello World script at 
// https://github.com/hosseinmoein/DataFrame/blob/master/examples/hello_world.cc
// proved useful in writing this code.

using ULDataFrame = StdDataFrame<unsigned long>;

ULDataFrame df_bts;

df_bts.load_data(std::move(bts_index_col),
std::make_pair("UNIQUE_CARRIER",  unique_carrier_col),
std::make_pair("ORIGIN",  origin_col),
std::make_pair("PASSENGERS",  passengers_col),
std::make_pair("DISTANCE", distance_col),
std::make_pair("DEPARTURES_PERFORMED",  departures_performed_col)
);

std::cout << "Finished creating our C++ DataFrame. Now performing analyses.";

// Creating a pivot table that shows average distances by airlines:
// This code was based on the examples shown at 
/* https://htmlpreview.github.io/?https://github.com/hosseinmoein/
DataFrame/blob/master/docs/HTML/groupby.html . */
auto df_airlines_by_dist = df_bts.groupby1<std::string>("UNIQUE_CARRIER",
LastVisitor<ULDataFrame::IndexType, ULDataFrame::IndexType>(),
std::make_tuple("DISTANCE", "mean_distance", MeanVisitor<double>()));

// Saving this table to a local .csv file:
// This code was based on the documentation found at 
// https://htmlpreview.github.io/?https://github.com/hosseinmoein/
// DataFrame/blob/master/docs/HTML/write.html .
// The addition of {.columns_only = true} instructs the DataFrame library
// not to include the index of df_airlines_by_dict within the file.
df_airlines_by_dist.write<std::string, double, 
std::size_t, int>("../Output/airlines_by_mean_dist.csv", io_format::csv2,
{.columns_only = true});

/* Creating a pivot table that shows the number of passengers served by 
each airline at each origin airport:
(This can help us identify the largest airline hubs in the US.) */

auto df_hubs_pivot = df_bts.groupby2<std::string, std::string>(
    "UNIQUE_CARRIER", "ORIGIN",
LastVisitor<ULDataFrame::IndexType, ULDataFrame::IndexType>(),
std::make_tuple("PASSENGERS", "origin_passengers", SumVisitor<double>()));

df_hubs_pivot.write<std::string, double, std::size_t, int>(
    "../Output/total_origin_pax_by_airline_and_airport.csv", 
io_format::csv2, {.columns_only = true});

}