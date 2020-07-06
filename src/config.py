import configparser
import os
from dotenv import load_dotenv, find_dotenv

ROOT_PATH = os.path.dirname(__file__)
load_dotenv(find_dotenv())
config = configparser.ConfigParser()
config.read(os.path.join(ROOT_PATH, 'dwh.cfg'))
config.add_section("AWS")
config.set("AWS", "KEY", os.getenv("AWS_KEY"))
config.set("AWS", "SECRET", os.getenv("AWS_SECRET"))
