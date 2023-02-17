import os
from os.path import expanduser
from pathlib import Path
from typing import Union

ACM_PREFIX = 'ACM-'
DROPBOX_PATH = expanduser('~/Dropbox (Amplio)')


def canonical_acm_path(acm: str, dropbox:Union[bytes, str, os.PathLike]=DROPBOX_PATH) -> Path:
    """
    Given an ACM or Program name return a Path to the canonical pathname. Pass 'dropbox' to override
    the default dropbox path. Update DROPBOX_PATH to override for all callers.
    """
    return Path(dropbox, canonical_acm_name(acm))


def canonical_acm_name(acm: str) -> str:
    """
    Given an ACM or Program name, return a canonicalized ACM name (upper case, with ACM- prefix).
    """
    if acm is None:
        return None
    acm = acm.upper()
    _, acm_name = os.path.split(acm)
    acm_name = acm_name.upper()
    if not acm_name.startswith(ACM_PREFIX):
        acm_name = ACM_PREFIX + acm_name
    return acm_name


def canonical_acm_program_name(acmdir: str) -> str:
    """
    Given an ACM or Program name, return a canonicalized Program name (upper case, no ACM- prefix).
    """
    if acmdir is None:
        return None
    _, program_name = os.path.split(acmdir)
    program_name = program_name.upper()
    if program_name.startswith(ACM_PREFIX):
        program_name = program_name[4:]
    return program_name
