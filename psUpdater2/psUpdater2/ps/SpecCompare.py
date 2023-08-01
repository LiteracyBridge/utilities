from typing import List, Dict

from ps.spec import ProgramSpec, Deployment, Playlist, Message, Recipient


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


class SpecCompare:
    def __init__(self, a: ProgramSpec, b: ProgramSpec):
        self.a: ProgramSpec = a
        self.b: ProgramSpec = b

    # def diff_deployments(self) -> List[str]:
    #     """
    #     Returns the diff between the deployments in the two program specs.
    #     :return: a List[str] with the diffs.
    #     """
    #
    #     def list_depl(d: Deployment, delta: str) -> None:
    #         d_dict = d.todict # asdict(d)
    #         values = [f'{k}: {_pr_val(d_dict[k])}' for k in d_dict.keys() if
    #                   k != 'deploymentnumber']
    #         results.append(f' Deployment # {d.deploymentnumber} {delta}: {", ".join(values)}')
    #
    #     def diff_depl(a: Deployment, b: Deployment) -> None:
    #         a_d = a.todict # asdict(a)
    #         b_d = b.todict # asdict(b)
    #         depls_changed: List[str] = [f for f in a_d.keys() if a_d[f] != b_d[f]]
    #         changes = [f"{x}: {_pr_diff(a_d[x], b_d[x])}" for x in depls_changed]
    #         results.append(f' Deployment # {a.deploymentnumber} changed: {", ".join(changes)}')
    #
    #     # Ensure that the deployments are compared in deployment-number order.
    #     a_depl_nums = sorted([d.deploymentnumber for d in self.a.deployments])
    #     a_dict = {d.deploymentnumber: d for d in self.a.deployments}
    #     b_depl_nums = sorted([d.deploymentnumber for d in self.b.deployments])
    #     b_dict = {d.deploymentnumber: d for d in self.b.deployments}
    #
    #     added: List[int] = [num for num in b_depl_nums if num not in a_depl_nums]
    #     removed: List[int] = [num for num in a_depl_nums if num not in b_depl_nums]
    #     changed: List[int] = [num for num in a_depl_nums if num in b_depl_nums and a_dict[num] != b_dict[num]]
    #
    #     results: List[str] = []
    #     for num in changed:
    #         diff_depl(a_dict[num], b_dict[num])
    #     for num in added:
    #         list_depl(b_dict[num], delta='added')
    #     for num in removed:
    #         list_depl(a_dict[num], delta='removed from progspec')
    #
    #     return results

    def diff_recipients(self) -> List[str]:
        """
        Retun the diff between the recipients in the two program specs.
        :return: A List[str] with the diffs.
        """

        def list_recip(r: Recipient, delta: str) -> None:
            r_dict = r.todict
            fullname = '/'.join(
                [r_dict[n] for n in ['region', 'district', 'communityname', 'groupname', 'agent'] if r_dict[n]])
            values = [f'{k}: {_pr_val(r_dict[k])}' for k in r_dict.keys() if
                      k not in ['region', 'district', 'recipientid', 'communityname', 'groupname', 'agent', 'affiliate',
                                'partner', 'component']]
            results.append(f' Recipient {r.recipientid} "{fullname}" {delta}: {", ".join(values)}')

        def diff_recip(a: Recipient, b: Recipient) -> None:
            a_d = a.todict
            b_d = b.todict
            columns_changed: List[str] = [f for f in a_d.keys() if a_d[f] != b_d[f]]
            changes = [f"{x}: {_pr_diff(a_d[x], b_d[x])}" for x in columns_changed]
            fullname = '/'.join([a_d[n] for n in ['communityname', 'groupname', 'agent'] if a_d[n]])
            results.append(f' Recipient {a.recipientid} "{fullname}" changed: {", ".join(changes)}')

        a_dict: Dict[str, Recipient] = {r.recipientid: r for r in self.a.recipients}
        b_dict: Dict[str, Recipient] = {r.recipientid: r for r in self.b.recipients}
        added: List[str] = [recip_id for recip_id in b_dict.keys() if recip_id not in a_dict]
        removed: List[str] = [recip_id for recip_id in a_dict.keys() if recip_id not in b_dict]
        changed: List[str] = [recip_id for recip_id in a_dict.keys() if
                              recip_id in b_dict and a_dict[recip_id] != b_dict[recip_id]]

        results: List[str] = []
        for recip_id in changed:
            diff_recip(a_dict[recip_id], b_dict[recip_id])
        for recip_id in added:
            list_recip(b_dict[recip_id], delta='added')
        for recip_id in removed:
            list_recip(a_dict[recip_id], delta='removed from progspec')
        return results

    def diff_content(self) -> List[str]:
        """
        Returns the differences between the content calendars of the two program specs. The content is
        sorted into deployments and playlists, and the comparisons are made at those levels.
        :return: A List[str] of the diffs.
        """

        def list_message(message: Message, delta: str = '') -> None:
            msg = {k: _pr_val(v) for k, v in message.todict.items() if v and k not in ['position', 'title']}
            msg_str = ', '.join([f'{k}: {v}' for k, v in msg.items()])
            results.append(f'  Message @ {message.position} "{message.title}" {delta}: {msg_str}')

        def list_playlist(playlist: Playlist, *, deploymentnumber: int, delta: str = '') -> None:
            results.append(
                f' Playlist "{playlist.title}" @ {playlist.position} in deployment # {deploymentnumber} {delta}:')
            for message in playlist.messages:
                list_message(message)

        def list_deployment(depl: Deployment, delta: str = '') -> None:
            """
            Deployments in the content, not from the Deployments tab.
            :param delta: What sort of change is being listed? 'Added'? 'Removed'?
            :param depl: The deployment.
            """
            playlist: Playlist
            for playlist in depl.playlists:
                list_playlist(playlist, deploymentnumber=depl.deploymentnumber, delta=delta)

        def diff_message(a: Message, b: Message) -> None:
            # What fields changed in the message? (Can't be the title, because that's how we say it's the same message.)
            a_dict = a.todict # asdict(a)
            b_dict = b.todict # asdict(b)
            changed: List[str] = [f for f in a_dict.keys() if a_dict[f] != b_dict[f] and (a_dict[f] or b_dict[f])]
            changes = [f"{x}: {_pr_diff(a_dict[x], b_dict[x])}" for x in changed]
            results.append(f'  Message "{a.title}": {", ".join(changes)}')

        def diff_playlist(a: Playlist, b: Playlist, deploymentnumber: int) -> None:
            # dict by title, because title is what user thinks in terms of
            a_msgs = {msg.title: msg for msg in a.messages}
            b_msgs = {msg.title: msg for msg in b.messages}
            # dict of fields, to show any changes to the playlist fields.
            a_map = {k: v for k, v in a.todict.items() if k not in 'messages'}
            b_map = {k: v for k, v in b.todict.items() if k not in 'messages'}
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

        def diff_deployment(a: Deployment, b: Deployment) -> None:
            a_map = a.todict
            b_map = b.todict
            changed: List[str] = [f for f in a_map.keys() if a_map[f] != b_map[f] and (a_map[f] or b_map[f])]
            changes = [f"{x}: {_pr_diff(a_map[x], b_map[x])}" for x in changed]

            a_plists = {pl.title: pl for pl in a.playlists}
            b_plists = {pl.title: pl for pl in b.playlists}


            # The only properties of deployments in the content tab are the playlists.
            playlists_added: List[str] = [title for title in b_plists.keys() if title not in a_plists]
            playlists_removed: List[str] = [title for title in a_plists.keys() if title not in b_plists]
            playlists_changed: List[str] = [title for title in a_plists.keys() if
                                            title in b_plists and a_plists[title] != b_plists[title]]

            results.append(f'Deployment "{a.deploymentname or b.deploymentname or a.deployment}" deployment # {a.deploymentnumber} changed: {", ".join(changes)}')
            for title in playlists_changed:
                diff_playlist(a_plists[title], b_plists[title], deploymentnumber=a.deploymentnumber)
            for title in playlists_added:
                list_playlist(b_plists[title], delta='added', deploymentnumber=a.deploymentnumber)
            for title in playlists_removed:
                list_playlist(a_plists[title], delta='removed', deploymentnumber=a.deploymentnumber)

        results: List[str] = []
        # These are deployments from the content tab, not the deployments tab.
        a_deployments: Dict[int, Deployment] = {d.deploymentnumber: d for d in self.a.deployments}
        b_deployments: Dict[int, Deployment] = {d.deploymentnumber: d for d in self.b.deployments}
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

    def diff(self, content_only: bool = False) -> List[str]:
        result: List[str] = []
        # changes = self.diff_deployments()
        # if len(changes) > 0:
        #     result.append('Deployments')
        #     result.extend(changes)
        changes = self.diff_content()
        if len(changes) > 0:
            result.append('Content')
            result.extend(changes)

        if not content_only:
            changes = self.diff_recipients()
            if len(changes) > 0:
                result.append('Recipients')
                result.extend(changes)

        return result

def compare_program_specs(a: ProgramSpec, b: ProgramSpec) -> List[str]:
    return SpecCompare(a, b).diff()