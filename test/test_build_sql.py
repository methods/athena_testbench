import shutil
import tempfile
import pytest

from typing import Sequence

import build_sql as module_under_test


# ===== FIXTURES ===== #


def string_sequence_to_file(str_seq: Sequence, filepath: str):
    """
    Write string sequence str_seq as lines to filepath.
    """
    with open(filepath, 'w') as f:
        f.write("\n".join(str_seq))


def file_to_string_sequence(filepath: str):
    """
    Write string sequence str_seq as lines to filepath.
    """
    with open(filepath, 'r') as f:
        return f.read().splitlines()


# ===== CASES ===== #


def test_build_sql_interpolates_single_insert_w_trailing_comma(tmp_path):
    """
    Assert build_sql constructs file given template file referencing a single
    insert, with a trailing comma.
    """
    inserted_sql_strs = [
        "SELECT",
        "   x AS i",
        "   y AS j",
        "FROM some_table"
    ]
    inserted_sql_filename = "inserted_sql.sql"
    inserted_sql_filepath = f"{tmp_path}/{inserted_sql_filename}"

    template_sql_strs = [
        f"WITH X AS @{inserted_sql_filename},",
        "SELECT some_stuff FROM somewhere;"
    ]
    template_sql_filename = "template_sql.sql"
    template_sql_filepath = f"{tmp_path}/{template_sql_filename}"

    expected_sql_strs = [
        f"WITH X AS",
        *inserted_sql_strs[:-1],
        f"{inserted_sql_strs[-1]},",
        "SELECT some_stuff FROM somewhere;"
    ]

    string_sequence_to_file(inserted_sql_strs, inserted_sql_filepath)
    string_sequence_to_file(template_sql_strs, template_sql_filepath)

    returned_sql_filename ="returned_sql.sql"
    returned_sql_filepath = f"{tmp_path}/{returned_sql_filename}"

    module_under_test.build_sql(template_file = template_sql_filepath,
                                output_file = returned_sql_filepath)

    returned_sql_strs = file_to_string_sequence(returned_sql_filepath)

    assert returned_sql_strs == expected_sql_strs


def test_build_sql_interpolates_single_insert_w_trailing_statement(tmp_path):
    """
    Assert build_sql constructs file given template file referencing a single
    insert, with a trailing statement.
    """
    inserted_sql_strs = [
        "SELECT",
        "   x AS i",
        "   y AS j",
        "FROM some_table"
    ]
    inserted_sql_filename = "inserted_sql.sql"
    inserted_sql_filepath = f"{tmp_path}/{inserted_sql_filename}"

    template_sql_strs = [
        f"WITH X AS @{inserted_sql_filename} AND Y AS Z",
        "SELECT some_stuff FROM somewhere;"
    ]
    template_sql_filename = "template_sql.sql"
    template_sql_filepath = f"{tmp_path}/{template_sql_filename}"

    expected_sql_strs = [
        f"WITH X AS",
        *inserted_sql_strs[:-1],
        f"{inserted_sql_strs[-1]}",
        "AND Y AS Z",
        "SELECT some_stuff FROM somewhere;"
    ]

    string_sequence_to_file(inserted_sql_strs, inserted_sql_filepath)
    string_sequence_to_file(template_sql_strs, template_sql_filepath)

    returned_sql_filename ="returned_sql.sql"
    returned_sql_filepath = f"{tmp_path}/{returned_sql_filename}"

    module_under_test.build_sql(template_file = template_sql_filepath,
                                output_file = returned_sql_filepath)

    returned_sql_strs = file_to_string_sequence(returned_sql_filepath)

    assert returned_sql_strs == expected_sql_strs


def test_build_sql_interpolates_single_insert_w_trailing_statement_and_comma(tmp_path):
    """
    Assert build_sql constructs file given template file referencing a single
    insert, with a trailing comma AND a trailing statement.
    """
    inserted_sql_strs = [
        "SELECT",
        "   x AS i",
        "   y AS j",
        "FROM some_table"
    ]
    inserted_sql_filename = "inserted_sql.sql"
    inserted_sql_filepath = f"{tmp_path}/{inserted_sql_filename}"

    template_sql_strs = [
        f"WITH X AS @{inserted_sql_filename}, AND Y AS Z",
        "SELECT some_stuff FROM somewhere;"
    ]
    template_sql_filename = "template_sql.sql"
    template_sql_filepath = f"{tmp_path}/{template_sql_filename}"

    expected_sql_strs = [
        f"WITH X AS",
        *inserted_sql_strs[:-1],
        f"{inserted_sql_strs[-1]},",
        "AND Y AS Z",
        "SELECT some_stuff FROM somewhere;"
    ]

    string_sequence_to_file(inserted_sql_strs, inserted_sql_filepath)
    string_sequence_to_file(template_sql_strs, template_sql_filepath)

    returned_sql_filename ="returned_sql.sql"
    returned_sql_filepath = f"{tmp_path}/{returned_sql_filename}"

    module_under_test.build_sql(template_file = template_sql_filepath,
                                output_file = returned_sql_filepath)

    returned_sql_strs = file_to_string_sequence(returned_sql_filepath)

    assert returned_sql_strs == expected_sql_strs


@pytest.mark.skip()
def test_build_sql_interpolates_multiple_inserts_and_writes(tmp_path):
    """
    Assert build_sql constructs file given template file referencing multiple
    inserts.
    """
    inserted_sql_strs_0 = [
        "SELECT",
        "   x AS i",
        "   y AS j",
        "FROM some_table"
    ]
    inserted_sql_0_filename = "inserted_sql.sql"
    inserted_sql_0_filepath = f"{tmp_path}/{inserted_sql_0_filename}"

    inserted_sql_strs_1 = [
        "SELECT",
        "   this AS that",
        "   the_other AS the_cats_mother",
        "FROM some_other_table"
    ]
    inserted_sql_1_filename = "inserted_sql.sql"
    inserted_sql_1_filepath = f"{tmp_path}/{inserted_sql_1_filename}"

    inserted_sql_strs_2 = [
        "SELECT",
        "   bill AS ben",
        "   dick AS harry",
        "FROM yet_another_table"
    ]
    inserted_sql_2_filename = "inserted_sql.sql"
    inserted_sql_2_filepath = f"{tmp_path}/{inserted_sql_2_filename}"


    template_sql_strs = [
        f"WITH X AS @{inserted_sql_filename},",
        "SELECT some_stuff FROM somewhere;"
    ]
    template_sql_filename = "template_sql.sql"
    template_sql_filepath = f"{tmp_path}/{template_sql_filename}"

    expected_sql_strs = [
        f"WITH X AS",
        *inserted_sql_strs,
        ",",
        "SELECT some_stuff FROM somewhere;"
    ]

    string_sequence_to_file(inserted_sql_strs_1, inserted_sql_1_filename)
    string_sequence_to_file(template_sql_strs, template_sql_filepath)

    returned_sql_filename ="returned_sql.sql"
    returned_sql_filepath = f"{tmp_path}/{returned_sql_filename}"

    module_under_test.build_sql(template_file = template_sql_filepath,
                                output_file = returned_sql_filepath)

    returned_sql_strs = file_to_string_sequence(returned_sql_filepath)

    assert returned_sql_strs == expected_sql_strs


