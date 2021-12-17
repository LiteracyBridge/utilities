from datetime import date
from typing import Dict, List, Tuple, Set

import Spec


# noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlResolve
class Validator:
    def __init__(self, deployments: List[Dict[str, str]], recipients: List[Dict[str, str]],
                 content: List[Dict[str, str]]):
        self._deployments: List[Dict[str, str]] = deployments
        self._recipients: List[Dict[str, str]] = recipients
        self._content: List[Dict[str, str]] = content

    @property
    def content(self):
        return self._content

    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validates the deployments, recipients, and content records.
        May modify those with minor corrections.
        :return: A tuple of OK and a list of messages.
        """

        def validate_record(record: Dict[str, str], required_fields: List[str], id_fields: List[str], name: str,
                            seen_names: Set[Tuple[str]], *, skip_duplicates: bool = False,
                            date_fields: List[str] = None) -> bool:
            """
            Validate one record from a program spec spreadsheet.
            :param record: The record to be validated.
            :param required_fields: List of required fields for this record type.
            :param id_fields: List of fields that make up the "name" or "id" of the record.
            :param name: Name of the record type, like "recipient". Used for error reporting.
            :param seen_names: Keep track of which id's have been seen here.
            :return: True if the record is valid, False if not.
            """
            if date_fields:
                for df in date_fields:
                    if df in record and isinstance(record[df], date):
                        record[df] = Spec.asdate(record[df])

            names = [str(y) if y else '' for y in [record[x] if x in record else '-' for x in id_fields]]
            missing = [x for x in required_fields if x not in record]
            if len(missing) > 0:
                errors.append(f'Missing fields for {name} "{"/".join(names)}": {", ".join(missing)}')
            else:
                if tuple(names) in seen_names:
                    if skip_duplicates:
                        return False
                    errors.append(f'Duplicate {name}: "{"/".join(names)}".')
                else:
                    seen_names.add(tuple(names))
                    return True
            return False

        errors: List[str] = []

        # Check Deployments for completeness & uniqueness.
        deployment_names = set()
        deployment_numbers = set()
        for deployment in self._deployments:
            if validate_record(deployment, Spec.deployment_required_fields, Spec.deployment_id_fields, 'deployment',
                               deployment_names, date_fields=Spec.date_columns):
                deployment_numbers.add(int(deployment.get('deploymentnumber')))

        # Check recipients
        recip_names = set()
        for recip in self._recipients:
            validate_record(recip, Spec.recipient_required_fields, Spec.recipient_id_fields, 'recipient', recip_names)

        # Check content
        content_names = set()
        keepers = []
        for content in self._content:
            if validate_record(content, Spec.content_required_fields, Spec.content_id_fields, 'content', content_names,
                               skip_duplicates=True):
                keepers.append(content)
                deployment_number = int(content.get('deployment_num'))
                if deployment_number not in deployment_numbers:
                    errors.append(f'Message requires missing deployment # {deployment_number}.')
        if len(keepers) != len(self._content):
            # TODO: alternative: self._content = keepers
            self._content.clear()
            self._content.extend(keepers)

        return len(errors) == 0, errors

    def populate_programspec(self, program: Spec.Program):
        for d in self._deployments:
            program.add_deployment(d)
        for m in self._content:
            program.add_content(m)
        for r in self._recipients:
            program.add_recipient(r)
