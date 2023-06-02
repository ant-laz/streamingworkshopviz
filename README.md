# streamingworkshopviz
Visualization of a streaming pipeline built using Apache Beam in Python

![overview of dashboard](/images/dash_preview_3.gif)

## python environment setup

```
# install python version 3.10.0 which is required for this lab
# one option is to use pyenv to help with this
# please use whather python installation method you prefer
pyenv shell 3.10.0

# create a virtual environment
python -m venv venv

# Activate the virtual environment.
source venv/bin/activate
```
## install python dependencies

```
# It's always a good idea to update pip before installing dependencies.
pip install -U pip

# Install the project as a local package, this installs all the dependencies as well.
pip install -r requirements.txt
```

## visualize the pipeline for 01_complete_ride

for the following inputs

```
streamlit run myviz.py -- \
'data/input/01_complete_ride/input.json' \
'data/output/01_complete_ride/output.json'
```

There is a single ride_id = 277278c9-9e5e-4aa9-b1b9-3e38e2133e5f

This ride has a complete trip of pickup, enroute & drop off events.

The dashboard displays the following:

![image of dashboard for 01_complete_ride](/images/01_complete_ride_dashboard.png)

Scrolling down the dashboard display what the data looks like throughout the pipeline:

![image of pipeline details view](/images/01_complete_ride_pipeline_details.png)

## visualize the pipeline for 05min_of_rides

for the following inputs

```
streamlit run myviz.py -- \
'data/input/05min_of_rides/input.json' \
'data/output/05min_of_rides/*'
```

There are many rides ! 

The dashboard displays the following:

![image of dashboard for 05min_of_rides](/images/05min_of_rides_dashboard.png)

Using the input box at the top it is possible to filter down to a single ride.

Let's choose ride_id = ea4ea9ce-a71d-4d5e-be65-1a68842b94f2

This ride has a complete trip of pickup, enroute & drop off events.

Now the dashboard only display data for this particular ride.
