import os
import re

def file_to_lines(filepath: str,
                  dir_if_not_in_filepath: str or None = None) -> list:
    split_filepath = os.path.split(filepath)

    if split_filepath[0] == '':
        if not dir_if_not_in_filepath:
            raise Exception(
                "Either filepath with directory or valid "
                "dir_if_not_in_filepath must be supplied as args"
            )

        filepath = os.path.join(dir_if_not_in_filepath, filepath)

    with open(filepath,'r') as f:
        return f.read().splitlines()


def build_sql(template_file, output_file, insert_delimiter='@') -> None:
    """
    NB no spaces in paths for inserted files in template file.

    Assume insert files are all in same dir as template file unless stated
    otherwise

    trailing commas and trailing statemetns (sep by whitespace) are handled
    """
    working_dir = os.path.split(template_file)[0]

    output_lines = []

    for line in file_to_lines(template_file):

        if insert_delimiter not in line:
            output_lines.append(line)
            continue

        line_split_on_delimiter = [statement.strip() for statement in
                                   line.split(insert_delimiter)]

        leading_statment = line_split_on_delimiter[0]
        if leading_statment != '':
            output_lines.append(leading_statment)

        files_to_insert = line_split_on_delimiter[1:]

        # last file to insert may have trailing content after whitespace
        end_sequence_of_line = files_to_insert[-1].split(' ', 1)
        files_to_insert[-1] = end_sequence_of_line[0]

        for file_to_insert in files_to_insert:

            # handle trailing commas
            if file_to_insert.endswith(','):
                lines_to_insert = file_to_lines(file_to_insert[:-1],
                                                working_dir)
                lines_to_insert[-1] += ','

            else:
                lines_to_insert = file_to_lines(file_to_insert, working_dir)

            output_lines.extend(lines_to_insert)

        if len(end_sequence_of_line) != 1:
            trailing_statement = end_sequence_of_line[1]
            output_lines.append(trailing_statement)

    with open(output_file, 'w') as f:
            f.write("\n".join(output_lines))






    # open and read inerted files
