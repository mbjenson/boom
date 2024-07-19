# downloads and extracts alerts from ztf archive
from urllib.request import urlretrieve
import tarfile
import os.path as Path

dirpath = Path.dirname(__file__)

print("downloading alerts...")
url = "https://ztf.uw.edu/alerts/public/ztf_public_20240326.tar.gz"
filename = dirpath + "/ztf_public_20240326.tar.gz"
path, res = urlretrieve(url, filename)
print("extracting archive...")
file = tarfile.open(filename)
file.extractall(dirpath + "/alerts")
file.close
print(f"successfully extracted alerts to {dirpath}/alerts")
