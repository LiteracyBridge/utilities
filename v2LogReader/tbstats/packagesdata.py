"""
Reads and porses a packagedata.txt file from a TBv2 deployent.
"""
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Tuple, Optional, Union


class PackagesDataException(Exception):
    pass

@dataclass
class Message():
    position: int
    pathix: int
    filename: str
    title: str = None

    @property
    def id(self) -> str:
        if self.filename:
            return Path(self.filename).stem


@dataclass
class Playlist():
    position: int
    title: str
    announcement_pathix: int
    announcement_filename: str
    invitation_pathix: int
    invitation_filename: str
    messages: List[Message] = field(default_factory=list)

    def get_message(self, ix):
        return self.messages[ix] if ix>=0 and ix<len(self.messages) else None


@dataclass
class Package():
    name: str
    announcement_pathix: int
    announcement_filename: str
    # path(s) with system prompts for this package
    prompts_paths: List[int] = field(default_factory=list)
    playlists: List[Playlist] = field(default_factory=list)

    def find_playlist(self, playlist_title):
        """
        Find a playlist in a package, by title.
        :param playlist_title: Playlist to be found
        :return: The Playlist if found, None if not
        """
        return next((x for x in self.playlists if x.title == playlist_title), None)

    def get_playlist(self, ix):
        if ix >= len(self.playlists):
            return None
        return self.playlists[ix]

@dataclass
class Deployment:
    name: str
    paths: List[str] = field(default_factory=list)
    packages: List[Package] = field(default_factory=list)

    def find_package(self, package_name):
        """
        Find a package in a deployment, by Title.
        :param package_name: Package name to be found.
        :return: The Package if found, None otherwise.
        """
        return next((x for x in self.packages if x.name == package_name), None)

    def get_package(self, ix):
        if ix >= len(self.packages):
            return None
        return self.packages[ix]

    def get_message_by_index(self, package_ix, playlist_ix, message_ix):
        if (package := self.get_package(package_ix)) is None:
            return None
        if (playlist := package.get_playlist(playlist_ix)) is None:
            return None
        return playlist.get_message(message_ix)

def read_packages_data(file: Path, **kwargs) -> Deployment:
    line_number = 0
    def get_line(with_comment=False) -> Union[str, Optional[Tuple[str, str]]]:
        nonlocal line_number
        while True:
            line_number += 1
            try:
                raw_line = data_file.readline()
                line = (''.join([chr(x) for x in raw_line])).strip()
            except Exception as ex:
                print(f'Error reading line {line_number} from packages_data.txt in {kwargs.get("collected_data_zip_name","??")}')
            if not line:
                return None
            parts = [x.strip() for x in line.split('#')]
            if len(parts[0]) > 0:
                if with_comment:
                    if len(parts) > 2:
                        return parts[0], parts[1]
                    return (parts[0],'') # caller expects a tuple
                return parts[0]

    def get_audio():
        audio_str, comment = get_line(with_comment=True)
        audio_parts = [x.strip() for x in audio_str.split()]
        return audio_parts[0], audio_parts[1], comment

    def read_message(position: int) -> Message:
        pathix, filename, title = get_audio()
        message: Message = Message(position, pathix, filename, title)
        return message

    def read_playlist(position: int) -> Playlist:
        title = get_line()
        # short prompt (announcement), long prompt (invitation)
        a_pathix, a_filename, _ = get_audio()
        i_pathix, i_filename, _ = get_audio()
        playlist: Playlist = Playlist(position, title, a_pathix, a_filename, i_pathix, i_filename)
        # There can be an arbitrary number of non-numeric flags and directives here. The count of messages
        # follows. Skip flags until we find an all-numeric line.
        num_messages = None
        while num_messages is None:
            line = get_line()
            if line.isdigit():
                num_messages = int(line)
            else:
                # some flag that we don't care about -- if we ever do, parse it here
                pass
        for n in range(0, num_messages):
            playlist.messages.append(read_message(n + 1))
        return playlist

    def read_package() -> Package:
        # package name
        name = get_line()
        # announcement
        pathix, filename, _ = get_audio()
        # path ix(s) of system prompts for this package
        prompts_paths = []
        try:
            pathixs_str = get_line()
            prompts_paths: List[int] = [int(x) for x in pathixs_str.split(';')]
        except Exception as ex:
            print(f'Unable to parse path list for package {name}: {pathixs_str}')
        package: Package = Package(name, pathix, filename, prompts_paths)
        num_playlists = int(get_line())
        for n in range(0, num_playlists):
            package.playlists.append(read_playlist(n + 1))
        return package

    def read_header() -> int:
        nonlocal result
        # format version
        version = get_line()
        if version != '1':
            raise Exception(f'Unknown format version: {version}')
        # deployment name
        deployment_name = get_line()
        result = Deployment(deployment_name)
        # number of paths
        num_paths = int(get_line())
        # the paths themselves
        for n in range(0, num_paths):
            result.paths.append(get_line())
        # number of packages
        return int(get_line())

    result: Deployment = None
    if not file.exists():
        raise PackagesDataException()
    with file.open('rb') as data_file:
        num_packages = read_header()
        for n in range(0, num_packages):
            result.packages.append(read_package())

    return result
