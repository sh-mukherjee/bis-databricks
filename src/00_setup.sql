-- Databricks notebook source
-- 00_setup: one-time Unity Catalog objects for BIS cashless pipeline.
-- Run on Serverless compute or the 2X-Small SQL Warehouse.

CREATE CATALOG IF NOT EXISTS bis COMMENT 'BIS CPMI payments analytics (Red Book)';

CREATE SCHEMA IF NOT EXISTS bis.payments COMMENT 'Cashless payments medallion tables (bronze/silver/gold)';

CREATE VOLUME IF NOT EXISTS bis.payments.raw COMMENT 'Raw BIS bulk zips and extracted CSVs for CPMI cashless pipeline';

-- Verify
SHOW VOLUMES IN bis.payments;
