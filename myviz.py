import streamlit as st
import numpy as np
import pandas as pd
import duckdb
import argparse

def viz(input: str, output: str):
    # create a duckdb cursor that we'll use to read JSON files.
    cursor = duckdb.connect()

    # load the pipeline input data
    inputfile = input
    inputquery = (
    "SELECT *" 
    "FROM read_json("
    f"    {inputfile}," 
    "    format=newline_delimited,"
    "    columns={"
    "        ride_id: 'VARCHAR',"
    "        point_idx: 'BIGINT',"
    "        latitude: 'DOUBLE',"
    "        longitude: 'DOUBLE',"
    "        timestamp: 'TIMESTAMP',"
    "        meter_reading: 'DOUBLE',"
    "        meter_increment: 'DOUBLE',"
    "        ride_status: 'VARCHAR',"
    "        passenger_count: 'BIGINT'"
    "    }"
    ");"
    )
    inputdf = cursor.execute(inputquery).df()

    # load the pipeline output data
    outputfile = output
    outputquery = (
    "SELECT *" 
    "FROM read_json("
    f"    {outputfile}," 
    "    format=newline_delimited,"
    "    columns={"
    "        ride_id: 'VARCHAR',"
    "        duration: 'DOUBLE',"
    "        min_timestamp: 'TIMESTAMP',"
    "        max_timestamp: 'TIMESTAMP',"
    "        count: 'BIGINT',"
    "        init_status: 'VARCHAR',"
    "        end_status: 'VARCHAR',"
    "        trigger: 'VARCHAR',"
    "        window_start: 'TIMESTAMP',"
    "        window_end: 'TIMESTAMP'"
    "    }"
    ");"
    )
    outputdf = cursor.execute(outputquery).df()

    # introduce the dashboard to users
    st.title(":taxi: Step by step development of a streaming pipeline in Python.")
    st.write("This is a simple visulization of the pipeline built during the workshop.")

    # widget to let users choose which ride_id to inspect & analyze
    chosenride = st.text_input(
        label="Which ride_id to analyze?",
        value=""
    )

    # dashboard for our taxi company
    mapdf = inputdf.copy(deep=True)
    left_column, right_column = st.columns(2)
    if chosenride == "":
        with left_column:
            # visualize all the taxi rides in this dataset
            manyridesmapdf = mapdf.copy(deep=True)
            manyridesmapdf = manyridesmapdf[manyridesmapdf["ride_status"]=="pickup"]
            st.map(manyridesmapdf)
        with right_column:
            # visualize stats about this single taxi ride on a map 
            st.metric(
                label="Number of rides",
                value=outputdf["ride_id"].nunique()
            )
            st.metric(
                label="Median stops per ride",
                value="{:.2f}".format(outputdf["count"].median())
            )                        
            st.metric(
                label="Median Duration of ride (seconds)",
                value="{:.2f}".format(outputdf["duration"].median())
            )
            st.metric(
                label="Mean stops per ride",
                value="{:.2f}".format(outputdf["count"].mean())
            )                        
            st.metric(
                label="Mean Duration of ride (seconds)",
                value="{:.2f}".format(outputdf["duration"].mean())
            )            
    else:
        with left_column:
            # visualize this single taxi ride on a map
            st.map(mapdf[mapdf["ride_id"]==chosenride])
        
        with right_column:
            # visualize stats about this single taxi ride on a map 
            st.metric(
                label="Number of rides",
                value=1
            )
            st.metric(
                label="Stops on the ride",
                value=outputdf[outputdf["ride_id"]==chosenride]["count"]
            )                        
            st.metric(
                label="Duration of ride (seconds)",
                value=outputdf[outputdf["ride_id"]==chosenride]["duration"]
            )           
    

    # visualise the pipeline input
    st.text("1. Pipeline input")
    if chosenride == "":
        st.write(inputdf)
    else:
        st.write(inputdf[inputdf["ride_id"]==chosenride])

    # visualize the timestamping & keying of PCollection elements
    timekeydf = inputdf.copy(deep=True)
    timekeydf = timekeydf[["point_idx","ride_id","timestamp"]]
    timekeydf.rename(columns={
        "ride_id":"key_for_pcollection_element",
        "timestamp":"timetsamp_for_pcollection_element"
        },
        inplace=True)
    st.text("2. Timestamp input & key identification")
    if chosenride == "":
        st.write(timekeydf)
    else:
        st.write(timekeydf[timekeydf["key_for_pcollection_element"]==chosenride])

    # visualize the windowing of a PCollection of elements
    windowdf = outputdf.copy(deep=True)
    windowdf = windowdf[["ride_id","window_start","window_end"]]
    st.text("3. Windowing")
    if chosenride == "":
        st.write(windowdf)
    else:
        st.write(windowdf[windowdf["ride_id"]==chosenride])

    # visualize the computation of statistics per window of PCollection of elements.
    st.text("4. Statistics per window")
    if chosenride == "":
        st.write(outputdf)
    else:
        st.write(outputdf[outputdf["ride_id"]==chosenride])

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'input',
        help="location of pipeline input files",
        type=str
        )
    parser.add_argument(
        'output',
        help="location of pipeline output files",
        type=str
        )
    args = parser.parse_args()
    viz(f"'{args.input}'", f"'{args.output}'")