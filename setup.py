#setup.py

import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
import csv
import pyfiglet
import time
import sys
import py2exe
from distutils.core import setup

setup(console=['dump_strains.py'])