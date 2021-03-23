import os
import subprocess

from pyetltools import get_default_logger


class Cmd():

    def __init__(self, executable, directory="", working_dir=None, args=[],env_override={}, shell=False):
        self.directory = directory
        self.executable = executable
        self.env_override = env_override
        self.original_env_values={}
        self.working_dir=working_dir
        self.default_args=args
        self.default_shell=shell
        self.logger= get_default_logger()

    def save_env(self):
        for key, value in self.env_override.items():
            if key in os.environ:
                self.original_env_values[key] = os.environ[key]

    def restore_env(self):
        for key, value in self.original_env_values.items():
            os.environ[key] = value

    def init_env(self):
        for key, value in self.env_override.items():
            print("SET "+key+"="+value)
            os.environ[key] = value

    def get_executable_full_path(self):
        print(self.directory)
        print(self.executable)
        return os.path.join(self.directory, self.executable)

    def run(self, *args, shell=None):
        self.save_env()
        self.init_env()

        if shell is None:
            if self.default_shell is not None:
                shell=self.default_shell
            else:
                shell=False

        if len(args)>0:
            args=[self.get_executable_full_path()] + list(args)
        else:
            args = [self.get_executable_full_path()] + list(self.default_args)

        self.logger.debug("Command:"+" ".join(args))
        self.logger.debug("Working dir:"+str(self.working_dir))
        res = subprocess.run(args,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.working_dir, shell=shell)
        self.restore_env()
        self.logger.debug("STDOUT:")
        stdout=res.stdout.decode("latin-1")
        self.logger.debug(stdout)
        self.logger.debug("STDERR:")
        stderror=res.stderr.decode("latin-1")
        self.logger.debug(stderror)
        self.logger.debug("RESULT:")
        self.logger.debug(res.returncode)
        if res.returncode != 0:
            raise Exception("Command failed with error code:"+str(res.returncode))
        return res
