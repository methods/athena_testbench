import argparse
import os
import re

from typing import Sequence

SNIPPET_LINE = "/* START SNIPPET */"


def file_to_lines(filepath: str,
                  dir_if_not_in_filepath: str or None = None, prefix: str = '') -> list:
    """
    Return text file at filepath as list of lines. If filepath does not contain
    a directory, then it must be supplied as dir_if_not_in_filepath, or an
    exception will be thrown.
    """
    split_filepath = os.path.split(filepath)

    if split_filepath[0] == '':
        if not dir_if_not_in_filepath:
            raise ValueError(
                "Either filepath with directory or valid "
                "dir_if_not_in_filepath must be supplied as args"
            )

        filepath = os.path.join(dir_if_not_in_filepath, filepath)

    with open(filepath, 'r') as f:
        lines = f.read().splitlines()
        return [f'{prefix}{line}' for line in lines]


def file_to_snippet(filepath: str,
                    dir_if_not_in_filepath: str or None = None, prefix: str = '') -> list:
    lines = file_to_lines(filepath=filepath, dir_if_not_in_filepath=dir_if_not_in_filepath, prefix='')

    try:
        position_of_snippet_line = lines.index(SNIPPET_LINE)
        return [f'{prefix}{line}' for line in lines[position_of_snippet_line + 1:]]
    except ValueError:
        return [f'{prefix}{line}' for line in lines]


def build_sql(template_file: str,
              output_file: str,
              insert_delimiter: str = '@',
              suffix_start_chars: Sequence = (',')) -> None:
    """
    Assemble sql script from template_file and write to output_file. The
    template_file can be an absolute path or a path relative to the location
    that this function is executed from (should be project root).

    Statements bounded by the insert_delimiter are assumed to point at some
    other sql script to interpolate in. e.g., using the default
    insert_delimiter, this will insert the contents of some_sql_script's 1 to 3
    between the @ symbols:

        WITH X AS @some_sql_script_1.sql@,
        WITH Y AS @some_sql_script_2.sql@,
        WITH Z AS @some_sql_script_3.sql@,
        "SELECT some_stuff FROM somewhere;

    Files referenced in between the insert_delimiters can either be full
    filepaths, or path-less filenames. If path-less filenames, they will be
    presumed to be in the same directory as the template_file.

    Statements immediately following an insert statement will be placed on a
    a new line of the compiled script, UNLESS they start with one of the
    suffix_start_chars, in which case they will be appended to the last line
    of the inserted script (e.g. by default trailing comma will be preserved
    as line terminators, rather than placed on new lines).
    """
    template_file = os.path.abspath(template_file)
    working_dir = os.path.split(template_file)[0]

    output_lines = []

    def _suffix_or_newline(statement: str) -> None:
        """
        If statement starts with any of the suffix_start_chars its a suffix,
        otherwise it goes in as a newline.
        """
        if any(statement.startswith(char) for char in suffix_start_chars):
            output_lines[-1] += statement
        else:
            output_lines.append(statement)

    for i, line in enumerate(file_to_lines(template_file, working_dir)):

        if SNIPPET_LINE in line:
            continue

        n_delimiters = len(re.findall(insert_delimiter, line))

        if n_delimiters == 0:
            output_lines.append(line)
            continue

        if n_delimiters % 2 != 0:
            raise ValueError(
                f"Unbalanced insert statement found on line {i} of {template_file}"
                f" - insert statements must be bookended by {insert_delimiter}"
            )

        line_split_on_delimiter = line.split(insert_delimiter)

        leading_statement = line_split_on_delimiter.pop(0)
        trailing_statement = line_split_on_delimiter.pop()

        if leading_statement != '':
            output_lines.append(leading_statement)

        for j, statement in enumerate(line_split_on_delimiter):

            if statement == '':
                continue

            # even index statements are files to insert
            if j % 2 == 0:
                output_lines.extend(
                    file_to_snippet(statement, working_dir, '  ')
                )
                continue

            # odd index statements are interstitial statements, either suffixes
            # or newlines
            _suffix_or_newline(statement)

        if trailing_statement != '':
            _suffix_or_newline(trailing_statement)

    with open(output_file, 'w') as f:
        f.write("\n".join(output_lines))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('template_file', type=str)
    parser.add_argument('output_file', type=str)
    args = parser.parse_args()
    build_sql(args.template_file, args.output_file)
