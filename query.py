# coding: utf-8

"""
data gathering
"""

from google.cloud import bigquery
from datetime import timedelta,datetime,date
import yaml,os
import uuid
import time
import calendar

conversion_base_90days= str(/queries/conversion_base_90days.sql)
