import re
from datetime import date
from typing import Dict, List, Tuple, Set, Callable, Any

import Spec


# noinspection SqlDialectInspection,SqlNoDataSourceInspection,SqlResolve
class Validator:
    def __init__(self, deployments: List[Dict[str, str]], recipients: List[Dict[str, str]],
                 content: List[Dict[str, str]]):
        self._deployments: List[Dict[str, str]] = deployments
        self._recipients: List[Dict[str, str]] = recipients
        self._content: List[Dict[str, str]] = content

    @property
    def content(self) -> List[Dict[str, str]]:
        return self._content

    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validates the deployments, recipients, and content records.
        May modify those with minor corrections.
        :return: A tuple of OK and a list of messages.
        """

        def ids(record: Dict[str, str], id_fields: List[str]) -> List[str]:
            return [str(record[x]) if x in record and record[x] else ' ' for x in id_fields]

        # {record_type : [id,...]}
        all_ids_seen: Dict[str, Set] = {}
        header_shown: Set = set()
        requireds_shown: Set = set()

        def _validate_record(record: Dict[str, str], record_type: str, required_fields: List[str], id_fields: List[str],
                             sql_2_csv: Dict[str, str],
                             extra_validation: Callable[..., Tuple[bool, List[str]]] = None) -> bool:
            """
            Validates a record according to the required fields and uniqueness of id fields.
            :param record: to be validated.
            :param record_type: name of the record type, like 'content'.
            :param required_fields: list of fields that must have values.
            :param id_fields: list of the fields that make up the id.
            :param sql_2_csv: a map of the internal names to the human-friendly names.
            :param date_fields: an optional list of fields that are dates. These are coerced to 'date' type.
            :param extra_validation: an optional function to perform additional validation.
            :return: True if the record passes validation, False otherwise.
            """
            def make_display_name() -> str:
                """
                Make a string suitable for showing to people, hopefully sufficient to identify the record being
                validated. Essentially a concatenation of id fields. Missing required id fields are shown as
                " (no field-name) "; missing non-required fields are omitted.
                :return: The human-friendly name.
                """
                display_names = []
                for id in id_fields:
                    if id in record and record[id]:
                        display_names.append(str(record[id]))
                    elif id in required_fields:
                        display_names.append(f' (no {sql_2_csv[id]}) ')
                return '/'.join(display_names)

            def show_header():
                """
                Shows the header for this record type, if it has not already been shown.
                """
                if record_type not in header_shown:
                    header_shown.add(record_type)
                    errors.append(f'Validating {record_type.title()}')

            def show_required():
                """
                Displays the required fields for this record type, if not already done.
                """
                show_header()
                if record_type not in requireds_shown:
                    requireds_shown.add(record_type)
                    errors.append(f'  These fields are required for {record_type.title()}: ' +
                                  f'{", ".join([sql_2_csv[f] for f in required_fields])}')

            def report_missing():
                """
                Reports one or more missing fields from the record.
                """
                show_required()
                errors.append(f'    Required {record_type.title()} field(s) are missing for ' +
                              f'"{make_display_name()}". Missing: {", ".join(missing)}')

            def report_duplicate():
                """
                Reports a duplicate record, based on the id fields.
                """
                show_header()
                errors.append(f'    Duplicate {record_type.title()}: "{make_display_name()}".')

            result: bool = True

            ids_seen = all_ids_seen.setdefault(record_type, set())
            # The values that uniquely identify this record.
            names = ids(record, id_fields)
            # Validations
            # Names of missing or empty required fields.
            missing = [sql_2_csv[x] for x in required_fields if x not in record or not record[x]]
            if len(missing) > 0:
                result = report_missing() and False
            if tuple(names) in ids_seen:
                result = report_duplicate() and False
            else:
                ids_seen.add(tuple(names))
            if result and extra_validation is not None:
                ok, issues = extra_validation(record, make_display_name())
                result = ok and result
                if len(issues) > 0:
                    show_header()
                    errors.extend(issues)
            return result

        def check_deployments():
            def check_depl_dates(record: Dict[str,Any], name: str) -> Tuple[bool, List[str]]:
                issues = []
                for df in date_fields:
                    if df in record and isinstance(record[df], date):
                        record[df] = Spec.asdate(record[df]) # Coerce to date without time.
                    else:
                        dep = f'Depl # {record.get("deploymentnumber", "-unknown-")}"'
                        val = f': "{record[df]}"' if record[df] else ''
                        issues.append(f'    Missing or malformed date "{df}" for {dep}{val}. Please use YYYY-MM-DD format.')
                return len(issues)==0, issues

            date_fields = [x for x in Spec.date_columns if x in Spec.deployment_fields]

            # Check Deployments for completeness & uniqueness.
            for deployment in self._deployments:
                if _validate_record(deployment, 'deployments', Spec.deployment_required_fields,
                                    Spec.deployment_id_fields, Spec.deployment_sql_2_csv,
                                    extra_validation=check_depl_dates):
                    deployment_numbers.add(int(deployment.get('deploymentnumber')))

        def check_content():
            def check_for_depl(record, name) -> Tuple[bool, List[str]]:
                deployment_number = int(record.get('deployment_num'))
                if deployment_number not in deployment_numbers:
                    return False, [
                        f'    Message "{name}" requires {Spec.content_sql_2_csv["deployment_num"]} {deployment_number}, which is not in the Deployments sheet.']
                return True, []

            keepers = []
            for content in self._content:
                if _validate_record(content, 'content', Spec.content_required_fields, Spec.content_id_fields,
                                    Spec.content_sql_2_csv, extra_validation=check_for_depl):
                    keepers.append(content)
                    variants.update([x.strip() for x in (content.get('variant')or'').split(',')])

            if len(keepers) != len(self._content):
                # TODO: alternative: self._content = keepers
                self._content.clear()
                self._content.extend(keepers)

        def check_recipients():
            def check_variants(record: Dict[str, Any], name: str) -> Tuple[bool, List[str]]:
                variant = record.get('variant')
                variant = variant.strip() if variant else ''
                if variant and variant not in variants:
                    if not v_pat.match(variant):
                        return False, [f'    Recipient {name} has a malformed variant: "{variant}". A variant shoule be letters ' +
                                       'and numbers only, and a recipient may have only one variant.']
                    return False, [f'    Recipient {name} has variant type {variant}, which is not a variant type of any content.']
                return True, []
            v_pat = re.compile('^\w+$')

            for recip in self._recipients:
                _validate_record(recip, 'recipients', Spec.recipient_required_fields, Spec.recipient_id_fields,
                                 Spec.recipient_sql_2_csv, extra_validation=check_variants)

        # Filled by check_deployments, then used by check_content. Ergo, must check deployments first.
        deployment_numbers = set()
        variants = set()
        errors: List[str] = []
        check_deployments()
        check_content()
        check_recipients()

        return len(errors) == 0, errors

    def populate_programspec(self, program: Spec.Program):
        for d in self._deployments:
            program.add_deployment(d)
        for m in self._content:
            program.add_content(m)
        for r in self._recipients:
            program.add_recipient(r)
