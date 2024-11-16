# Databricks notebook source
# MAGIC %sql
# MAGIC WITH av_super AS (
# MAGIC   SELECT Position, AVG(Superstar) as average
# MAGIC   from jdc_draft_2015
# MAGIC   GROUP BY Position
# MAGIC   )
# MAGIC
# MAGIC   SELECT p.Player, p.NameID, p.Superstar, p.Position, a.average
# MAGIC   FROM jdc_draft_2015 p 
# MAGIC   JOIN av_super a 
# MAGIC   ON p.Position = a.Position
# MAGIC   WHERE Superstar > a.average
# MAGIC   ORDER BY Position, Superstar DESC
# MAGIC
