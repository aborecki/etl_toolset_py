import os
import subprocess


class Cmd():

    def __init__(self, executable, directory="", env_override={}):
        self.directory = directory
        self.executable = executable
        self.env_override = env_override
        self.original_env_values={}

    def save_env(self):
        for key, value in self.env_override.items():
            if key in os.environ:
                self.original_env_values[key] = os.environ[key]

    def restore_env(self):
        for key, value in self.original_env_values.items():
            os.environ[key] = value

    def init_env(self):
        for key, value in self.env_override.items():
            os.environ[key] = value

    def get_executable_full_path(self):
        print(self.directory)
        print(self.executable)
        return os.path.join(self.directory, self.executable)

    def run(self, *args):
        self.save_env()
        self.init_env()
        args=[self.get_executable_full_path()] + list(args)
        print(" ".join(args))
        res = subprocess.run(args,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.restore_env()
        print("STDOUT:")
        stdout=res.stdout.decode("utf-8")
        print(stdout)
        print("STDERR:")
        stderror=res.stderr.decode("utf-8")
        print(stderror)
        return (stdout, stderror, res)
