# Databricks notebook source
files = dbutils.fs.ls("/mnt/data/bronze/citi_bike_data")

display(files)
