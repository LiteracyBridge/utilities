from dataclasses import asdict
from typing import List, Dict

import Spec
from Spec import Program


def _pr_val(x) -> str:
    """
    Format a value. Empty value and values with spaces are enclosed in quotes. Long values are elided.
    :param x: THe value to print.
    :return: The decoreated value.
    """
    x = str(x) if x is not None else ''
    x = x if len(x) <= 30 else x[0:27] + '...'
    return f"'{x}'" if x == '' or ' ' in x else x


def _pr_diff(a, b) -> str:
    """
    Decorates a from -> to pair.
    :param a: The from value.
    :param b: The to value.
    :return: The from->to pair, decorated.
    """
    return f"{_pr_val(a)} -> {_pr_val(b)}"


class SpecCompare():
    def __init__(self, a: Program, b: Program):
        self.a: Program = a
        self.b: Program = b

    def diff_deployments(self) -> List[str]:
        """
        Returns the diff between the deployments in the two program specs.
        :return: a List[str] with the diffs.
        """

        def list_depl(d: Spec.Deployment, delta: str) -> None:
            d_dict = asdict(d)
            values = [f'{k}: {_pr_val(d_dict[k])}' for k in d_dict.keys() if k != 'deploymentnumber']
            results.append(f' Deployment # {d.deploymentnumber} {delta}: {", ".join(values)}')

        def diff_depl(a: Spec.Deployment, b: Spec.Deployment) -> None:
            a_dict = asdict(a)
            b_dict = asdict(b)
            changed: List[str] = [f for f in a_dict.keys() if a_dict[f] != b_dict[f]]
            changes = [f"{x}: {_pr_diff(a_dict[x], b_dict[x])}" for x in changed]
            results.append(f' Deployment # {a.deploymentnumber} changed: {", ".join(changes)}')

        # Ensure that the deployments are compared in deployment-number order.
        a_depl_nums = sorted([d.deploymentnumber for d in self.a.deployments])
        a_dict = {d.deploymentnumber: d for d in self.a.deployments}
        b_depl_nums = sorted([d.deploymentnumber for d in self.b.deployments])
        b_dict = {d.deploymentnumber: d for d in self.b.deployments}

        added: List[int] = [num for num in b_depl_nums if num not in a_depl_nums]
        removed: List[int] = [num for num in a_depl_nums if num not in b_depl_nums]
        changed: List[int] = [num for num in a_depl_nums if num in b_depl_nums and a_dict[num] != b_dict[num]]

        results: List[str] = []
        for num in changed:
            diff_depl(a_dict[num], b_dict[num])
        for num in added:
            list_depl(b_dict[num], delta='added')
        for num in removed:
            list_depl(a_dict[num], delta='removed from progspec')

        return results

    def diff_recipients(self) -> List[str]:
        """
        Retun the diff between the recipients in the two program specs.
        :return: A List[str] with the diffs.
        """

        def list_recip(r: Spec.Recipient, delta: str) -> None:
            r_dict = asdict(r)
            fullname = '/'.join(
                [r_dict[n] for n in ['region', 'district', 'communityname', 'groupname', 'agent'] if r_dict[n]])
            values = [f'{k}: {_pr_val(r_dict[k])}' for k in r_dict.keys() if
                      k not in ['region', 'district', 'recipientid', 'communityname', 'groupname', 'agent', 'affiliate',
                                'partner', 'component']]
            results.append(f' Recipient {r.recipientid} "{fullname}" {delta}: {", ".join(values)}')

        def diff_recip(a: Spec.Recipient, b: Spec.Recipient) -> None:
            a_dict = asdict(a)
            b_dict = asdict(b)
            changed: List[str] = [f for f in a_dict.keys() if a_dict[f] != b_dict[f]]
            changes = [f"{x}: {_pr_diff(a_dict[x], b_dict[x])}" for x in changed]
            fullname = '/'.join([a_dict[n] for n in ['communityname', 'groupname', 'agent'] if a_dict[n]])
            results.append(f' Recipient {a.recipientid} "{fullname}" changed: {", ".join(changes)}')

        a_dict: Dict[str, Spec.Recipient] = {r.recipientid: r for r in self.a.recipients}
        b_dict: Dict[str, Spec.Recipient] = {r.recipientid: r for r in self.b.recipients}
        added: List[str] = [id for id in b_dict.keys() if id not in a_dict]
        removed: List[str] = [id for id in a_dict.keys() if id not in b_dict]
        changed: List[str] = [id for id in a_dict.keys() if id in b_dict and a_dict[id] != b_dict[id]]

        results: List[str] = []
        for id in changed:
            diff_recip(a_dict[id], b_dict[id])
        for id in added:
            list_recip(b_dict[id], delta='added')
        for id in removed:
            list_recip(a_dict[id], delta='removed from progspec')
        return results

    def diff_content(self) -> List[str]:
        """
        Returns the differences between the content calendars of the two program specs. The content is
        sorted into deployments and playlists, and the comparisons are made at those levels.
        :return: A List[str] of the diffs.
        """

        def list_message(message: Spec.DbMessage, delta: str = '') -> None:
            msg = {k: _pr_val(v) for k, v in asdict(message).items() if v and k not in ['position', 'title']}
            msg_str = ', '.join([f'{k}: {v}' for k, v in msg.items()])
            results.append(f'  Message @ {message.position} "{message.title}" {delta}: {msg_str}')

        def list_playlist(playlist: Spec.DbPlaylist, *, deploymentnumber: int, delta: str = '') -> None:
            results.append(
                f' Playlist "{playlist.title}" @ {playlist.position} in deployment # {deploymentnumber} {delta}:')
            for message in playlist.db_messages:
                list_message(message)

        def list_deployment(depl: Spec.DbDeployment, delta: str = '') -> None:
            """
            Deployments in the content, not from the Deployments tab.
            :param depl: The deployment.
            """
            playlist: Spec.DbPlaylist
            for playlist in depl.db_playlists.values():
                list_playlist(playlist, deploymentnumber=depl.deploymentnumber, delta=delta)

        def diff_message(a: Spec.DbMessage, b: Spec.DbMessage) -> None:
            # What fields changed in the message? (Can't be the title, because that's how we say it's the same message.)
            a_dict = asdict(a)
            b_dict = asdict(b)
            changed: List[str] = [f for f in a_dict.keys() if a_dict[f] != b_dict[f]]
            changes = [f"{x}: {_pr_diff(a_dict[x], b_dict[x])}" for x in changed]
            results.append(f'  Message "{a.title}": {", ".join(changes)}')

        def diff_playlist(a: Spec.DbPlaylist, b: Spec.DbPlaylist, deploymentnumber: int) -> None:
            # dict by title, because title is what user thinks in terms of
            a_msgs = {msg.title: msg for msg in a.db_messages}
            b_msgs = {msg.title: msg for msg in b.db_messages}
            # dict of fields, to show any changes to the playlist fields.
            a_map = {k: v for k, v in asdict(a).items() if k not in 'db_messages'}
            b_map = {k: v for k, v in asdict(b).items() if k not in 'db_messages'}
            changed: List[str] = [f for f in a_map.keys() if a_map[f] != b_map[f]]
            changes = [f"{x}: {_pr_diff(a_map[x], b_map[x])}" for x in changed]
            # Find differences in the actual playlist contents.
            messages_added: List[str] = [title for title in b_msgs.keys() if title not in a_msgs]
            messages_removed: List[str] = [title for title in a_msgs.keys() if title not in b_msgs]
            messages_changed: List[str] = [title for title in a_msgs.keys() if
                                           title in b_msgs and a_msgs[title] != b_msgs[title]]
            # It changed, or we wouldn't be here.
            results.append(f' Playlist "{a.title}" in deployment # {deploymentnumber} changed: {", ".join(changes)}')
            for title in messages_changed:
                diff_message(a_msgs[title], b_msgs[title])
            for title in messages_added:
                list_message(b_msgs[title], delta='added')
            for title in messages_removed:
                list_message(a_msgs[title], delta='removed')

        def diff_deployment(a: Spec.DbDeployment, b: Spec.DbDeployment) -> None:
            # The only properties of deployments in the content tab are the playlists.
            playlists_added: List[str] = [title for title in b.db_playlists.keys() if title not in a.db_playlists]
            playlists_removed: List[str] = [title for title in a.db_playlists.keys() if title not in b.db_playlists]
            playlists_changed: List[str] = [title for title in a.db_playlists.keys() if
                                            title in b.db_playlists and a.db_playlists[title] != b.db_playlists[title]]
            for title in playlists_changed:
                diff_playlist(a.db_playlists[title], b.db_playlists[title], deploymentnumber=a.deploymentnumber)
            for title in playlists_added:
                list_playlist(b.db_playlists[title], delta='added', deploymentnumber=a.deploymentnumber)
            for title in playlists_removed:
                list_playlist(a.db_playlists[title], delta='removed', deploymentnumber=a.deploymentnumber)

        results: List[str] = []
        # These are deployments from the content tab, not the deployments tab.
        a_deployments = Spec.flat_content_to_hierarchy(self.a)
        b_deployments = Spec.flat_content_to_hierarchy(self.b)
        # We don't actually care about deployments added/removed here, only about the playlists targeted for
        # those deployments. When reporting these changes, we'll state them in terms of playlists.
        depls_added: List[int] = [n for n in b_deployments.keys() if n not in a_deployments]
        depls_removed: List[int] = [n for n in a_deployments.keys() if n not in b_deployments]
        depls_changed: List[int] = [n for n in a_deployments.keys() if
                                    n in b_deployments and a_deployments[n] != b_deployments[n]]
        for n in depls_changed:
            diff_deployment(a_deployments[n], b_deployments[n])
        for n in depls_added:
            list_deployment(b_deployments[n], delta='added')
        for n in depls_removed:
            list_deployment(a_deployments[n], delta='removed')

        return results

    def diff(self, content_only:bool = False) -> List[str]:
        result: List[str] = []
        if not content_only:
            changes = self.diff_deployments()
            if len(changes) > 0:
                result.append('Deployments')
                result.extend(changes)
            changes = self.diff_recipients()
            if len(changes) > 0:
                result.append('Recipients')
                result.extend(changes)
        changes = self.diff_content()
        if len(changes) > 0:
            result.append('Content')
            result.extend(changes)

        return result
