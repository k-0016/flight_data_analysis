# Flight Data Analysis

This project implements an extensive analysis of the Airline On-time Performance dataset using Apache Hadoop and Oozie. It focuses on identifying airlines and airports with notable on-time performances and average taxi times, alongside the most common reasons for flight cancellations.

## Project Overview

Analyzed and Processed historical flight data spanning from October 1987 to April 2008 by setting up a distributed environment on AWS VMs.'

## Project Achievements

- **Completed a complex Oozie workflow** that successfully integrates three MapReduce jobs to analyze extensive airline performance data over 22 years.
- **Efficiently scaled the data analysis process** from two to the maximum allowed number of VMs, demonstrating the system's scalability and robustness in handling large datasets.

### Objectives

- Determine the three airlines with the highest and lowest on-time performance probabilities.
- Identify airports with the longest and shortest average taxi times.
- Analyze the most common reasons for flight cancellations.

## Installation

1. Set up Hadoop and Oozie on AWS VMs.
2. Download the dataset from the [Data Expo 2009 Airline on-time data](https://stat-computing.org/dataexpo/2009/the-data.html).

## Configuration

Modify the `job.properties` file to match your Hadoop and Oozie setup:

