# mg

## Overview

This repository contains code and resources for mortgage data analysis, including data loading, cleaning, transformation, and export functionalities. It is designed to handle the **Freddie Mac loan performance dataset**, which contains more than **600 million rows**—much larger than the memory capacity of my personal 8GB MacBook Air.  

To manage this scale, the analysis is conducted using **lazy evaluation with Polars**, so data is only loaded into memory when absolutely necessary. Model estimation is performed on **bootstrapped samples** to ensure robustness while avoiding the need to process the full dataset. Time-consuming steps and estimation tasks are **parallelized** for faster computation.  

## Features

- Load and preprocess Freddie Mac loan performance data  
- Export data to DuckDB and Parquet formats  
- Perform SQL-based data selection and transformation  
- Randomly sample loan records for scalable analysis  

## Requirements

- Python 3.8+  
- [DuckDB](https://duckdb.org/)  
- pandas  
- polars  

## Usage

1. Clone the repository:
   ```bash
   git clone https://github.com/yuyaogawa8/loan_forecast.git
   cd loan_forecast
