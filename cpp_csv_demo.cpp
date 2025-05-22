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
// Relevant documentation for these steps:
// Joins:
// https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/join_by_column.html

// Addition/subtraction:
//https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/global_operators.html
// (I could also try getting each column; adding those values together
// within a loop; and then adding this result as a new column--as these
// operations may be meant for entire DataFrames.)
// 3. Add code that shows the runtime for this file

// Add more documentation
# include <chrono>
auto program_start_time = std::chrono::high_resolution_clock::now();
    //Source: https://stackoverflow.com/a/47888078/13097194
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

std::cout << "Starting program.\n";

ThreadGranularity::set_optimum_thread_level();

bool use_full_file = true;

/* Sample file (for testing and debugging):
Because I'm building the final program within a /build folder, 
I needed to precede the filename with ../ so that the program 
would know to go up one level to access the file. */

auto data_import_start_time = std::chrono::high_resolution_clock::now();

std::string file_path = "";

if (use_full_file == true)
    file_path = "/home/kjb3/D1V1/Documents/Large_Datasets/BTS/T_\
T100_SEGMENT_ALL_CARRIER_2024.csv"; //Full 2024 file 
// (stored outside this project directory due to its size)
else
    file_path = "../T_T100_SEGMENT_ALL_CARRIER_2024_top_\
10k_rows_by_pax.csv";

CSVReader reader(file_path);


// Creating vectors that can store data for selected columns:
// (This approach allows us to only (1) read in data and (2) specify
//  names and types for certain columns,
// which can be useful when working with datasets that have 
// hundreds of fields.)

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
departures_performed_col.push_back(
    row["DEPARTURES_PERFORMED"].get<double>());
row_count += 1;
};

std::cout << "Finished importing data. Total row count: " << 
    row_count << "\n";

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

auto data_import_end_time = std::chrono::high_resolution_clock::now();
auto data_import_time = data_import_end_time - data_import_start_time;
std::cout << "Finished importing data and creating C++ DataFrame in " <<
std::chrono::duration<double>(data_import_time).count() <<
    " seconds. Now performing analyses.\n";

auto data_analysis_start_time = std::chrono::high_resolution_clock::now();

// Creating a pivot table that shows the average distance flown for each 
// airline's route: (this code weights each route equally.)
// This code was based on the examples shown at 
/* https://htmlpreview.github.io/?https://github.com/hosseinmoein/
DataFrame/blob/master/docs/HTML/groupby.html . */
auto df_airlines_by_dist = df_bts.groupby1<std::string>("UNIQUE_CARRIER",
LastVisitor<ULDataFrame::IndexType, ULDataFrame::IndexType>(),
std::make_tuple("DISTANCE", "mean_distance", MeanVisitor<double>()));
// Sorting this DataFrame by mean distance:
// (Documentation for sorting can be found at 
// https://htmlpreview.github.io/?https://github.com/hosseinmoein/DataFrame/blob/master/docs/HTML/sort.html )

// NOTE: This sort appears to only modify the mean_distance column,
// thus causing it and the UNIQUE_CARRIER column to get out of order.
// Planning to investigate this further. Commenting out this code
// in the meantime.
//df_airlines_by_dist.sort<double>("mean_distance",sort_spec::desce);

/* Creating a pivot table that shows the number of passengers served by 
each airline at each origin airport:
(This can help us identify the largest airline hubs in the US.) */

auto df_hubs_pivot = df_bts.groupby2<std::string, std::string>(
    "UNIQUE_CARRIER", "ORIGIN",
LastVisitor<ULDataFrame::IndexType, ULDataFrame::IndexType>(),
std::make_tuple("PASSENGERS", "origin_passengers", SumVisitor<double>()));
//df_hubs_pivot.sort<double>("origin_passengers",sort_spec::desce);

    
// Saving this table to a local .csv file:
// This code was based on the documentation found at 
// https://htmlpreview.github.io/?https://github.com/hosseinmoein/
// DataFrame/blob/master/docs/HTML/write.html .
// The addition of {.columns_only = true} instructs the DataFrame library
// not to include the index of df_airlines_by_dict within the file.
df_airlines_by_dist.write<std::string, double, 
std::size_t, int>("../Output/airlines_by_mean_dist.csv", io_format::csv2,
{.columns_only = true});

df_hubs_pivot.write<std::string, double, std::size_t, int>(
    "../Output/total_origin_pax_by_airline_and_airport.csv", 
io_format::csv2, {.columns_only = true});

auto data_analysis_end_time = std::chrono::high_resolution_clock::now();
auto data_analysis_time = data_analysis_end_time - data_analysis_start_time;
std::cout << "Finished analyzing data and writing output to .csv files in " << 
std::chrono::duration<double>(data_analysis_time).count() << " seconds.\n";


auto program_end_time = std::chrono::high_resolution_clock::now();
    //Source: https://stackoverflow.com/a/47888078/13097194

auto run_time = program_end_time - program_start_time;
std::cout << "Finished running program in " << 
std::chrono::duration<double>(run_time).count() << " seconds.\n";

//See https://en.cppreference.com/w/cpp/chrono/duration
// for more information on these different timing options.

}