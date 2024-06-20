from subprocess import Popen, PIPE, run, CalledProcessError
from os.path import join, isfile
from time import sleep
import requests
from typing import List, NewType
from types import MappingProxyType
from pkg_resources import resource_filename

# RunAccessionData = NewType("RunAccessionData", MappingProxyType)

# def fetch_accession_data(accession: str, attempts:int=5, delay:int=2) -> List[RunAccessionData]:
def fetch_accession_data(accession: str, attempts:int=5, delay:int=2) -> List[dict]:
    """
    Fetch all run_accession data for project, sample, or run accession.
    attempts: Number of times to try request
    delay: seconds between request attempts
    """
    params = dict(accession=accession, result="read_run", fields="all", format="json")
    attempts = 0
    while True:
        response = requests.get("http://www.ebi.ac.uk/ena/portal/api/filereport", params=params)
        if response.status_code == 200:
            data = response.json() 
            if data:
                return data
                # return [RunAccessionData(i) for i in data] # Ignore pycharm warning
            else:
                raise ValueError("No data returned for request")
            break
        elif response.status_code == 429:   
            if attempts == attempts:
                raise requests.HTTPError("Request rejected {attempts} times. Giving up.")
            else:
                print("Request rejected for \"Too Many Requests\". Trying again...")
                attempts += 1
                sleep(delay)
        else:
            print(response.content)
            raise requests.HTTPError(f"Error fetching data from ENA: [{response.status_code}] \"{response.text}\"")

def get_read_accession_fields():
    """
    Return list of all fields and field descriptors for run accession meta data.
    """
    response = requests.get("https://www.ebi.ac.uk/ena/portal/api/returnFields?result=read_run")
    if response.status_code == 200:
        return response.text
    else:
        raise requests.HTTPError(f"Error fetching data from ENA: [{response.status_code}] \"{response.text}\"")

def validate_md5(path: str, md5: str) -> bool:
    result = run(f"md5sum {path}", stdout=PIPE, shell=True, check=True)
    if md5 == result.stdout.decode("utf-8").split()[0]:
        return True
    else:
        return False

def download_fastq(url: str, outdir: str, method: str, md5: str = "", validate: bool = False, 
        rate_limit: int = 0, retries: int = 5, wait: int = 5, ascp_key: str = "", force: bool = False):
    """
    rate_limit: maximum allowed download rate
    retries: the number of download request to make before quitting
    wait: seconds to wait before retrying after failed download attempt
    ascp_key: path to ascp key
    force: force redownload of existing files if not validating checksums
    """
    outpath = join(outdir, url.split('/')[-1])
    # Check if file already exists and if download should be skipped.
    skip = False
    if isfile(outpath):
        if validate: 
            if md5:
                if validate_md5(outpath, md5):
                    skip = True
                    print(f"{outpath} with valid checksum already exists.")
            else:
                raise ValueError("Missing argument \"md5\". You must specify an md5 checksum in order to validate.")
        else:
            if not force:
                skip = True
                print(f"{outpath} already exists. Skipping download.")
    if not skip:
        # Construct download command
        limit = ""
        match method:
            case "aspera":
                if not url.startswith("fasp.sra.ebi.ac.uk:"):
                    raise ValueError("Unexpected url. Url should begin with fasp.sra.ebi.ac.uk:")
                if not ascp_key:
                    ascp_key = resource_filename("sratools", "data/asperaweb_id_dsa.openssh")
                if rate_limit > 0:
                    limit = f"-l {rate_limit}M"
                cmd = f"ascp {limit} -v -k 3 -T -P 33001 -i {ascp_key} era-fasp@{url} {outpath}"
            case "ftp":
                if not url.startswith("ftp.sra.ebi.ac.uk"):
                    raise ValueError("Unexpected url. Url should begin with ftp.sra.ebi.ac.uk")
                if rate_limit > 0:
                    limit = f"--limit-rate {rate_limit}"
                cmd = f"wget {limit}M -c -O {outpath} {url}"
        # Run download command
        attempts = 0
        while True:
            process = Popen(cmd, stdout=PIPE, universal_newlines=True, shell=True)
            for line in process.stdout:
                print(line, end="")
            process.wait()
            if process.returncode == 0:
                break
            else:
                if attempts == 5:
                    raise CalledProcessError(process.returncode, cmd, "", "Download failed after 5 attempts. Giving up")
                else:
                    print(f"Download attempt failed. Trying again...")
                    attempts += 1
                    sleep(wait)
        if validate: 
            if md5:
                if not validate_md5(outpath, md5):
                    raise Exception("Invalid file checksum computed for {outpath}") 
    print("Download complete.")

def fastq_urls(data, method: str) -> List[str]:
    match method:
        case "aspera":
            key = "fastq_aspera"
        case "ftp":
            key = "fastq_ftp"
    return data[key].split(';')
    
def fastq_md5s(data):
    return data["fastq_md5"].split(';')