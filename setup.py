# -*- coding: utf-8 -*-
"""
Created on Thu Oct 16 01:30:22 2025

@author: admin
"""

from setuptools import setup, find_packages

setup(
    name="zohoconfigloader",
    version="1.0.0",
    packages=find_packages(),
    py_modules=["AnalyticsClient"]  # This exposes AnalyticsClient.py at top level
)