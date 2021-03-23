from pyetltools.core.cmd import Cmd
from pyetltools.core.connector import Connector
import os
from functools import reduce

from pyetltools.hg.lib.hg_history import extract_files_history


class HgConnector(Connector):

    def __init__(self, key=None, root_folder=None, sub_folder=None):
        super().__init__(key)
        if root_folder is None:
            raise Exception("root_folder parameter is not set")
        self.root_folder = root_folder
        self.sub_folder = sub_folder
        self.set_sub_folder(sub_folder)

    def set_sub_folder(self, sub_folder):
        self.sub_folder = sub_folder
        working_dir = os.path.join(self.root_folder, sub_folder if sub_folder is not None else "")
        self.hg_command = Cmd("hg", working_dir=working_dir)
        self.working_dir = working_dir

    def get_working_dir(self):
        return self.working_dir

    def pull(self):
        self.hg_command.run("pull")

    def push(self):
        self.hg_command.run("push")

    def update(self):
        self.hg_command.run("update")

    def commit(self, message, file):
        self.hg_command.run("commit", "-m", message, file)

    def diff(self, arg):
        self.hg_command.run("diff", arg)

    def add(self, arg):
        self.hg_command.run("add", arg)

    def run(self, *args):
        return self.hg_command.run(*args)

    def add_tag_to_commit(self, repository, changeset_id, tag):
        hgtags_file = os.path.join(self.root_folder, repository, ".hgtags")
        print(f"Adding tag {tag} to {changeset_id}  in " + hgtags_file)
        # write the new line to the end - I used append to avoid checking for newline at the end of exisintg file
        with open(hgtags_file, 'r') as f:
            current_content = f.readlines()
        newline = f"{changeset_id} {tag}"

        if f"{changeset_id} {tag}" in [l.strip() for l in current_content]:
            print(f"{newline} already in file {hgtags_file}")
        else:
            with open(hgtags_file, 'w', newline="\n") as f:
                current_content.append(f"{newline}\n")
                f.writelines(current_content)

    def get_tags(self):
        out = self.hg_command.run("tags", "--debug")
        lines = out.stdout.decode("latin-1").splitlines()
        ret = []
        for l in lines:
            tag_ver, changesetid = l.split(":")
            tag, ver = tag_ver.split()
            ret.append((tag, ver, changesetid))
        return ret

    def get_log(self, filename=None):
        if filename is not None:
            out = self.hg_command.run("log", filename)
        else:
            out = self.hg_command.run("log")
        line_colection = out.stdout.decode("latin-1").split("\n\n")
        ret = []
        for lc in line_colection:
            lines = lc.splitlines()
            vals = []
            for l in lines:
                s = l.split(sep=None, maxsplit=1)
                key = s[0].replace(":", "")
                value = s[1]
                vals.append((key, value))
            if len(vals) > 0:
                ret.append(dict(vals))
        return ret

    def validate_config(self):
        return True


HgConnector.extract_files_history = extract_files_history
