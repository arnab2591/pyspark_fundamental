-- Databricks notebook source
--# We have uploaded the csv file to below location
--#/FileStore/tables/EmployeeData.csv


-- COMMAND ----------

DROP TABLE IF EXISTS employeeData;
create table employeeData USING CSV OPTIONS (path "/FileStore/tables/EmployeeData.csv",header "true")

-- COMMAND ----------

Select * from employeeData

-- COMMAND ----------

Select gender,avg(salary) as Avg_Salary from employeeData GROUP BY gender

-- COMMAND ----------

