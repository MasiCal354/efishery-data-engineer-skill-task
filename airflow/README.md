# Solution for the First Question

## Configure Environment Variables

Create and edit `.env` following example on `example.env`

## Initiate DB and Run Airflow

Execute `docker-compose up` or `docker-compose up -d` to keep an active terminal

## Manage Airflow via UI

Open `locahost:8080` on browser and login using credentials provided on `.env`

## Shut Down Airflow and Database

Execute `docker-compose down`

## Proposed Data Warehouse Design

[Activity Schema](https://dbdiagram.io/d/629a171754ce26352755f2a7) is super efficient to use on Columnar Data Warehouse, even though in this implementation we use Row Based Data Warehouse (due to time limitation), most of the data warehouse engine is using columnar storage. It also avoid data discrepancy as the data is stored inside a single table and not distributed around different data sources.
