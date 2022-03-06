import argparse
from os.path import expanduser
from pathlib import Path
from typing import Optional


class StorePathAction(argparse.Action):
    """
    An argparse.Action to store a Path object. A leading ~ is expanded to the user's home directory.

    if 'glob=True' is specified in the add_argument call, any path containing a '?' or '*' will be
    'globbed'. These must also specify 'nargs', so that the result is a list.
    """

    @staticmethod
    def _expand(v: str) -> Optional[Path]:
        """
        Does the work of expanding.
        :param v: A string, possibly with a leading ~ to be expanded ot user's home directory.
        :return: A Path object that encapsulates the given path. Note that there is no guarantee of
            any actual file system object at that path.
        """
        return v and Path(expanduser(v))  # 'None' if v is None, otherwise Path(expanduser(v))

    def __init__(self, option_strings, dest, default=None, **kwargs):
        if 'glob' in kwargs:
            self._glob = kwargs.get('glob', False)
            del kwargs['glob']
        else:
            self._glob = False
        super(StorePathAction, self).__init__(option_strings, dest, default=self._expand(default), **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        if self._glob and not isinstance(values, list): raise TypeError(
            "The 'glob' option requires that the value is a list; did you use 'nargs'?")
        if not isinstance(values, list):
            values = self._expand(values)
        else:
            result = []
            for value in values:
                path:Path = self._expand(value)
                path_str = str(path)
                if self._glob and ('*' in path_str or '?' in path_str):
                    import glob
                    result.extend([Path(x) for x in glob.glob(path_str)])
                    # TODO: use Path.glob() result.extend([x for x in path.glob()])
                else:
                    result.append(path)
            values = result
        setattr(namespace, self.dest, values)
