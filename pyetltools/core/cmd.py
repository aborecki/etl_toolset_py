import os
import subprocess


class Cmd():

    def __init__(self, executable, directory="",working_dir=None, env_override={}):
        self.directory = directory
        self.executable = executable
        self.env_override = env_override
        self.original_env_values={}
        self.working_dir=working_dir

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
        print("Command:"+" ".join(args))
        print("Working dir:"+str(self.working_dir))
        res = subprocess.run(args,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.working_dir)
        self.restore_env()
        print("STDOUT:")
        stdout=res.stdout.decode("latin-1")
        print(stdout)
        print("STDERR:")
        stderror=res.stderr.decode("latin-1")
        print(stderror)
        print("RESULT:")
        print(res.returncode)
        if res.returncode != 0:
            raise Exception("Command failed with error code:"+res.returncode)
        return res
