import argparse
import os
from os.path import expanduser
from pathlib import Path
from typing import Union

dropbox_directory: Path = None
def set_dropbox_directory(dir: Path) -> None:
    global dropbox_directory
    dropbox_directory = dir

def canonical_acm_dir_name(acm:str, upper=True) -> str:
    if upper:
        acm = acm.upper()
    if not acm.startswith('ACM-'):
        acm = 'ACM-' + acm
    return acm


def canonical_acm_path_name(acm: str, upper=True) -> Path:
    global dropbox_directory
    acm_path = Path(dropbox_directory ,canonical_acm_dir_name(acm, upper=upper))
    return acm_path


def canonical_acm_project_name(acmdir: str) -> Union[None,str]:
    if acmdir is None:
        return None
    _, acm = os.path.split(acmdir)
    acm = acm.upper()
    if acm.startswith('ACM-'):
        acm = acm[4:]
    return acm


class StorePathAction(argparse.Action):
    """
    An argparse.Action to store a Path object. A leading ~ is expanded to the user's home directory.
    """

    def _expand(self, v: str) -> Union[None, Path]:
        """
        Does the work of expanding.
        :param v: A string, possibly with a leading ~ to be expanded ot user's home directory.
        :return: A Path object that encapsulates the given path. Note that there is no guarantee of
            any actual file system object at that path.
        """
        if v is None:
            return None
        v = expanduser(v)
        return Path(v)

    def __init__(self, option_strings, dest, nargs=None, default=None, **kwargs):
        super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), nargs=nargs,
                                              **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        values = [self._expand(v) for v in values] if isinstance(values, list) else self._expand(values)
        setattr(namespace, self.dest, values)