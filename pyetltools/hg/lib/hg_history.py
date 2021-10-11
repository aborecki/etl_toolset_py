import os
from collections import namedtuple
from pathlib import Path

from pyetltools import logger


def save_file(output_dir, rel_path, rev, file_contents):
    path=output_dir+f"//"+str(rel_path)+f".r_{rev}"
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    logger.debug(f"Saving file to {path}")
    with open(path, 'wb') as file:
        file.write(file_contents.encode('latin-1'))
        file.close()
    return path

HGFileHist = namedtuple("HGFileHist", 'path rel_path rev commited_date author summary file_content')

def extract_files_history(hg_conn,files_rglob_regex, output_dir, file_filter=lambda filename, src: True):
    wd=hg_conn.get_working_dir()
    for path in Path(wd).rglob(files_rglob_regex):
        rel_path = path.relative_to(hg_conn.get_working_dir())
        logger.debug(rel_path)
        with open(path, 'r', encoding="windows-1252") as file:
            file_str = file.read()
        if file_filter(path, file_str):
            log = hg_conn.get_log(str(rel_path))
            for l in log:
                rev, _ = l["changeset"].split(":")
                commited_date = l["date"]
                author = l["user"]
                summary = l["summary"]
                print(f"Analyzing file {rel_path} for rev {rev}")
                f_content = hg_conn.run("cat", str(rel_path), f"-r {rev}").stdout.decode("latin-1")
                file_path = save_file(output_dir, rel_path, rev, f_content)
                yield  HGFileHist(str(path), str(rel_path), rev, commited_date, author, summary, f_content)
        else:
            print(f"File {path} has not sproc inside")
